package sqlstream

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/skillian/ctxutil"
	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/sqlstream/sqllang"
	sqlddl "github.com/skillian/sqlstream/sqllang/ddl"
)

type query interface {
	expr.NamedVar
	stream.Streamer
}

// queryComponent is a component of a query (e.g. a filter of a query,
// a join, etc.)
type queryComponent interface {
	query
	query() query
}

func tableQueryOf(q query) (tq *tableQuery, hops int) {
	for q != nil {
		var ok bool
		if tq, ok = q.(*tableQuery); ok {
			return
		}
		q = q.(queryComponent).query()
		hops++
	}
	return
}

func createQueryComponentName(qc queryComponent) string {
	tq, hops := tableQueryOf(qc.query())
	return fmt.Sprintf("%s_%d", tq.Name(), hops)
}

// table defines the set of data needed for a query
type table struct {
	// db is the database the query was created from
	db *DB

	// modelType holds information about the Go struct that results
	// from the query will be Scanned into
	modelType *modelType

	// sqlTable is the database table that the query comes from
	sqlTable *sqlddl.Table
}

type preparedQuery struct {
	data           *pqData
	executionCount int32
}

type pqData struct {
	stmt     *sql.Stmt
	preparer sqlPreparer
	args     []interface{}
}

func (pq *preparedQuery) executeQuery(ctx context.Context, q query) (stream.Stream, error) {
	tq, _ := tableQueryOf(q)
	db := tq.table.db
	preparer, ok := ctxutil.Value(ctx, (*sqlPreparer)(nil)).(sqlPreparer)
	if !ok {
		preparer = db.db
	}
	data := (*pqData)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pq.data))))
	if data != nil {
		var stmt *sql.Stmt
		if data.preparer == preparer {
			stmt = data.stmt
		} else if stmter, ok := preparer.(sqlStmter); ok {
			stmt = stmter.StmtContext(ctx, data.stmt)
		}
		if stmt != nil {
			return data.executeQueryStatement(ctx, tq.table, stmt)
		}
	}
	sb := strings.Builder{}
	sw, err := db.info.MakeSQLWriter(
		ctx,
		smallWriterCounterOf(&sb),
	)
	if err != nil {
		return nil, fmt.Errorf("building SQLWriter: %w", err)
	}
	sn, err := createSQLNodeFromQuery(ctx, q)
	if err != nil {
		return nil, fmt.Errorf(
			"create SQL AST from query: %#v: %w",
			q, err,
		)
	}
	_, err = sw.WriteSQL(ctx, sn)
	if err != nil {
		return nil, fmt.Errorf("writing SQL: %w", err)
	}
	if err := sb.WriteByte(';'); err != nil {
		return nil, fmt.Errorf(
			"failed to write terminating semicolon to %v: %w",
			sb, err,
		)
	}
	sqlStmtStr := sb.String()
	if db.shouldPreparer.shouldPrepare(ctx, q) {
		stmt, err := preparer.PrepareContext(ctx, sqlStmtStr)
		if err != nil {
			return nil, fmt.Errorf(
				"preparing SQL %v: %w",
				sqlStmtStr, err,
			)
		}
		data = &pqData{
			stmt:     stmt,
			args:     sw.Args(),
			preparer: preparer,
		}
		ptrPqData := (*unsafe.Pointer)(unsafe.Pointer(&pq.data))
		if !atomic.CompareAndSwapPointer(ptrPqData, nil, unsafe.Pointer(data)) {
			if err := data.stmt.Close(); err != nil {
				return nil, fmt.Errorf(
					"failed to close extra statement %v: %w",
					data.stmt, err,
				)
			}
			data = (*pqData)(atomic.LoadPointer(ptrPqData))
		}
		if data != nil {
			return data.executeQueryStatement(ctx, tq.table, stmt)
		}
		logger.Warn1(
			"failed to store or load prepared query data from %p!?",
			ptrPqData,
		)
	}
	args, err := getOrAppendSQLArgs(ctx, nil, sw.Args())
	if err != nil {
		return nil, fmt.Errorf(
			"getting args for %v: %w",
			q, err,
		)
	}
	rows, err := tq.table.db.db.QueryContext(ctx, sqlStmtStr, args...)
	if err != nil {
		return nil, fmt.Errorf(
			"executing query %v: %w",
			q, err,
		)
	}
	return createStreamFromRows(tq.table, rows)
}

func (data *pqData) executeQueryStatement(ctx context.Context, t table, stmt *sql.Stmt) (stream.Stream, error) {
	args, err := getOrAppendSQLArgs(ctx, nil, data.args)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return createStreamFromRows(t, rows)
}

func createStreamFromRows(t table, rows *sql.Rows) (stream.Stream, error) {
	return &rowsStream{
		modelType: t.modelType,
		rows:      rows,
		values:    make([]interface{}, 0, len(t.sqlTable.Columns)),
	}, nil
}

func createSQLNodeFromQuery(ctx context.Context, q query) (sn sqllang.Node, err error) {
	t := queryJoinTree{
		nodes:   make([]queryJoinTreeNode, 0, arbitraryCapacity),
		queries: make([]query, 0, arbitraryCapacity*arbitraryCapacity),
	}
	t.init(q, 0)
	src := &sqllang.Source{}
	err = initSQLSourceFromQueryTree(ctx, nil, src, t, 0)
	if err != nil {
		return nil, err
	}
	if src.Select != nil {
		return src.Select, nil
	}
	return src, nil
}

func initSQLSourceFromQueryTree(ctx context.Context, parent, src *sqllang.Source, t queryJoinTree, queryJoinTreeIndex int) error {
	joinsOf := func(src *sqllang.Source) *[]sqllang.Join {
		if src.Select != nil {
			src = &src.Select.From
		}
		return &src.Joins
	}
	promoteToSelect := func(src *sqllang.Source) *sqllang.Select {
		if src.Select == nil {
			sel := &sqllang.Select{
				From: *src,
			}
			*src = sqllang.Source{}
			src.Select = sel
		}
		return src.Select
	}
	// demoteToSource demotes a nested select within a
	// sqllang.Source to just a sqllang.Source, moving its JOINs,
	// WHERE expression, etc. into a parent SELECT.
	var demoteToSource func(parent *sqllang.Select, src *sqllang.Source)
	demoteToSource = func(parent *sqllang.Select, src *sqllang.Source) {
		sel := src.Select
		if sel == nil {
			return
		}
		demoteToSource(sel, &src.Select.From)
		if sel.Limit != nil {
			return
		}
		if len(sel.OrderBy) > 0 {
			return
		}
		if len(sel.Columns) > 0 {
			logger.Warn0(
				"maybe this should not be a condition " +
					"to deny \"demoting\"",
			)
			return
		}
		if sel.Where != (sqllang.Where{}) && parent == nil {
			return
		}
		src.Table, sel.From.Table = sel.From.Table, ""
		src.Alias, sel.From.Alias = sel.From.Alias, ""
		if src.Joins == nil {
			src.Joins = sel.From.Joins
		} else {
			src.Joins = append(src.Joins, sel.From.Joins...)
		}
		sel.From.Joins = nil
		if sel.Where != (sqllang.Where{}) {
			if parent.Where == (sqllang.Where{}) {
				parent.Where = sel.Where
			} else {
				parent.Where.Expr = expr.And{
					parent.Where.Expr,
					sel.Where.Expr,
				}
			}
			sel.Where = sqllang.Where{}
		}
	}
	elem := t.nodes[queryJoinTreeIndex]
	for i, q := range t.queries[elem.firstChild : elem.firstChild+elem.children] {
		switch q := q.(type) {
		case *filterQuery:
			sel := promoteToSelect(src)
			if sel.Where.Expr == nil {
				sel.Where.Expr = q.filter
			} else {
				sel.Where.Expr = expr.And{
					sel.Where.Expr,
					q.filter,
				}
			}
		case *joinQuery:
			joins := joinsOf(src)
			*joins = append(*joins, sqllang.Join{
				On: q.on,
			})
			joinSrc := &(*joins)[len(*joins)-1].Source
			if err := initSQLSourceFromQueryTree(
				ctx,
				src,
				joinSrc,
				t,
				elem.firstChild+i,
			); err != nil {
				return err
			}
			demoteToSource(src.Select, joinSrc)
		case *mapQuery:
			panic("// TODO: implement *mapQuery")
		case *sortQuery:
			sel := promoteToSelect(src)
			sel.OrderBy = append(sel.OrderBy, sqllang.Sort{
				By:   q.sort,
				Desc: q.descending,
			})
		case *tableQuery:
			if queryJoinTreeIndex == 0 || src.Select != nil {
				if err := initSQLSelectFromTableQuery(
					promoteToSelect(src), q,
				); err != nil {
					return err
				}
			} else {
				if err := initSQLSourceTableAndAliasFromTableQuery(
					src, q,
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func initSQLSelectFromTableQuery(sel *sqllang.Select, q *tableQuery) error {
	ddlCols := q.table.sqlTable.Columns
	sel.Columns = make([]sqllang.Column, len(ddlCols))
	t := reflect.New(q.table.modelType.unsafereflectType.ReflectType()).Interface()
	for i := range ddlCols {
		f := q.table.modelType.unsafereflectType.FieldPointer(t, i)
		sel.Columns[i] = sqllang.Column{
			Expr: expr.MemOf(q, t, f),
		}
	}
	return initSQLSourceTableAndAliasFromTableQuery(&sel.From, q)
}

func initSQLSourceTableAndAliasFromTableQuery(src *sqllang.Source, q *tableQuery) error {
	sb := strings.Builder{}
	sb.WriteByte('"')
	st := q.table.sqlTable
	if st.TableName.SchemaName.Name != "" {
		sb.WriteString(st.TableName.SchemaName.Name)
	}
	sb.WriteString(st.TableName.Name)
	sb.WriteByte('"')
	src.Table = sb.String()
	src.Alias = q.Name()
	return nil
}

type queryJoinTree struct {
	nodes   []queryJoinTreeNode
	queries []query
}

func (t *queryJoinTree) init(q query, queryIndex int) {
	start := len(t.queries)
	t.queries = appendQueryComponents(t.queries, q)
	end := len(t.queries)
	// swap the appended components so they go from inner to outer.
	for i := range t.queries[start : start+((end-start)/2)] {
		t.queries[start+i], t.queries[end-1-i] = t.queries[end-1-i], t.queries[start+i]
	}
	t.nodes = append(t.nodes, queryJoinTreeNode{
		query:      queryIndex,
		firstChild: start,
		children:   end - start,
	})
	for i, q := range t.queries[start:end] {
		if jq, ok := q.(*joinQuery); ok {
			if Debug {
				for _, q2 := range t.queries {
					if q == q2 {
						panic(fmt.Errorf(
							"recursive query detected: %v has already been added to the query tree: %#v",
							q, t.queries,
						))
					}
				}
			}
			t.init(jq.to, start+i)
		}
	}
}

type queryJoinTreeNode struct {
	// query is the index of the query object for this node
	// in the tree's nodes list
	query int

	// firstChild is the index of the first child of this query
	firstChild int

	// children is the
	children int
}

// tableQuery is a "root" query on a table
type tableQuery struct {
	table table

	prepared preparedQuery

	// name is the name ("alias") of the query
	name atomicString
}

func (q *tableQuery) preparedQuery() *preparedQuery { return &q.prepared }

var _ interface {
	query
	stream.Filterer
} = (*tableQuery)(nil)

func (q *tableQuery) Filter(ctx context.Context, e expr.Expr) stream.Streamer {
	return &filterQuery{
		from:   q,
		filter: e,
	}
}

func (q *tableQuery) Name() string {
	writeFirstRuneOfEachNamePart := func(sb *strings.Builder, name string) {
		for {
			part, suffix, ok := stringsCutTrimSpace(name)
			r, _ := utf8.DecodeRuneInString(part)
			_, _ = sb.WriteRune(r)
			if !ok {
				break
			}
			name = suffix
		}
	}
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		q := arg.(*tableQuery)
		sb := strings.Builder{}
		name := q.table.modelType.rawName.SchemaName.Name
		if name != "" {
			writeFirstRuneOfEachNamePart(&sb, name)
		}
		writeFirstRuneOfEachNamePart(&sb, q.table.modelType.rawName.Name)
		return sb.String()
	})
}

func (q *tableQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}

func (q *tableQuery) Var() expr.Var { return q }

type filterQuery struct {
	from     query
	prepared preparedQuery
	filter   expr.Expr
	name     atomicString
}

var _ interface {
	query
	queryComponent
} = (*filterQuery)(nil)

func (q *filterQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *filterQuery) query() query { return q.from }
func (q *filterQuery) Name() string {
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		return createQueryComponentName(arg.(queryComponent))
	})
}
func (q *filterQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}
func (q *filterQuery) Var() expr.Var { return q.from.Var() }

type joinQuery struct {
	from     query
	prepared preparedQuery
	to       query
	on       expr.Expr
	name     atomicString
}

var _ interface {
	query
	queryComponent
} = (*joinQuery)(nil)

func (q *joinQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *joinQuery) query() query { return q.from }
func (q *joinQuery) Name() string {
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		return createQueryComponentName(arg.(queryComponent))
	})
}
func (q *joinQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}
func (q *joinQuery) Var() expr.Var { return q }

type mapQuery struct {
	from     query
	prepared preparedQuery
	mapping  expr.Expr
	name     atomicString
}

var _ interface {
	query
	queryComponent
} = (*mapQuery)(nil)

func (q *mapQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *mapQuery) query() query { return q.from }
func (q *mapQuery) Name() string {
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		return createQueryComponentName(arg.(queryComponent))
	})
}
func (q *mapQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}
func (q *mapQuery) Var() expr.Var { return q }

type sortQuery struct {
	from       query
	prepared   preparedQuery
	sort       expr.Expr
	name       atomicString
	descending bool
}

var _ interface {
	query
	queryComponent
} = (*sortQuery)(nil)

func (q *sortQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *sortQuery) query() query { return q.from }
func (q *sortQuery) Name() string {
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		return createQueryComponentName(arg.(queryComponent))
	})
}
func (q *sortQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}
func (q *sortQuery) Var() expr.Var { return q.from.Var() }

// appendQueryComponents walks through the query to append to
// components where the last element is the root *tableQuery.
func appendQueryComponents(components []query, q query) []query {
	for {
		components = append(components, q)
		qc, ok := q.(queryComponent)
		if !ok {
			break
		}
		q = qc.query()
	}
	return components
}

func stringsCutTrimSpace(s string) (prefix, suffix string, ok bool) {
	i := strings.IndexFunc(s, unicode.IsSpace)
	if i == -1 {
		return s, "", false
	}
	j := strings.IndexFunc(s[i:], func(r rune) bool {
		return !unicode.IsSpace(r)
	})
	if j == -1 {
		return s[:i], "", true
	}
	return s[:i], s[i+j:], true
}

// getOrAppendSQLArgs returns queryArgs if none of queryArgs' elements
// are expr.Vars.  If any of queryArgs' elements are an expr.Var, then
// all queryArgs are appended to appendToArgs with their expr.Vars
// resolved with expr.Values retrieved from the Context.
//
// You can check for either condition by checking if
//
//	&args[0] == &queryArgs[0]
func getOrAppendSQLArgs(ctx context.Context, appendToArgs, queryArgs []interface{}) (args []interface{}, err error) {
	var vs expr.Values
	for i, arg := range queryArgs {
		if va, ok := arg.(expr.Var); ok {
			if args == nil {
				if cap(appendToArgs)-len(appendToArgs) < len(queryArgs) {
					appendToArgs2 := make([]interface{}, len(appendToArgs), len(appendToArgs)+len(queryArgs))
					copy(appendToArgs2, appendToArgs)
					appendToArgs = appendToArgs2
				}
				args = append(appendToArgs, queryArgs[:i]...)
			}
			if vs == nil {
				vs, err = expr.ValuesFromContext(ctx)
				if err != nil {
					return
				}
			}
			args[i], err = vs.Get(ctx, va)
			if err != nil {
				err = fmt.Errorf(
					"%v.Get(%v): %w",
					vs, va, err,
				)
				return
			}
			continue
		}
		if args != nil {
			args = append(args, arg)
		}
	}
	if args == nil {
		args = queryArgs
	}
	return
}

type rowsStream struct {
	query     query
	modelType *modelType
	rows      *sql.Rows
	values    []interface{}
}

var _ interface {
	stream.Stream
} = (*rowsStream)(nil)

func (rs *rowsStream) Next(ctx context.Context) (err error) {
	vs, err := expr.ValuesFromContext(ctx)
	if err != nil {
		return fmt.Errorf("cannot get next from %v: %w", rs, err)
	}
	va := rs.Var()
	v, err := vs.Get(ctx, va)
	if err != nil {
		return fmt.Errorf(
			"%v failed to get var %v from %v: %w",
			rs, va, vs, err,
		)
	}
	rs.values = rs.modelType.AppendFieldPointers(rs.values[:0], v)
	if err = rs.rows.Scan(rs.values...); err != nil {
		return fmt.Errorf(
			"%v error scanning row: %w",
			rs, err,
		)
	}
	return nil
}

func (rs *rowsStream) Var() expr.Var { return rs.query.Var() }
