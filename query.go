package sqlstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
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

// Query creates a `stream.Streamer“ that essentially starts as a
// `SELECT * FROM model` query.  Subsequent `stream.Filter`,
// `stream.Join`, etc. operations will result in modifications to this
// query.
func Query(ctx context.Context, model interface{}) stream.Streamer {
	return newTableQuery(modelTypeOf(model))
}

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

type preparedQuery struct {
	unsafeData     map[*DBInfo]*pqData
	executionCount int32
}

var emptyPQData = make(map[*DBInfo]*pqData, 0)

func (pq *preparedQuery) init() {
	pq.unsafeData = emptyPQData
}

// ensure that our assumption that a map is the size of a pointer
// for:
//
//	(*preparedQuery).data
//
// and
//
//	(*preparedQuery).tryGetOrAdd
func _(arr [1]byte) byte {
	return arr[unsafe.Sizeof(unsafe.Pointer(nil))-unsafe.Sizeof(map[*DBInfo]*pqData{})]
}

func (pq *preparedQuery) data() map[*DBInfo]*pqData {
	p := atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&pq.unsafeData)),
	)
	return *((*map[*DBInfo]*pqData)(unsafe.Pointer(&p)))
}

type dbInfoPQData struct {
	Key   *DBInfo
	Value *pqData
}

func (pq *preparedQuery) getOrAddData(kvps []dbInfoPQData) {
	missingKvps := make([]dbInfoPQData, 0, len(kvps))
	const maxSpins = 1024
	for spins := 0; spins < maxSpins; spins++ {
		m := pq.data()
		missingKvps = missingKvps[:0]
		for _, kvp := range kvps {
			existing, ok := m[kvp.Key]
			if ok {
				kvp.Value = existing
			} else {
				missingKvps = append(missingKvps, kvp)
			}
		}
		if len(missingKvps) == 0 {
			return
		}
		m2 := make(map[*DBInfo]*pqData, len(m)+len(missingKvps))
		for k, v := range m {
			m2[k] = v
		}
		for _, kvp := range missingKvps {
			m2[kvp.Key] = kvp.Value
		}
		if pq.casData(m, m2) {
			return
		}
	}
	panic(fmt.Errorf(
		"failed to swap preparedQuery data after %d spins",
		maxSpins,
	))
}

func (pq *preparedQuery) casData(old, new map[*DBInfo]*pqData) (swapped bool) {
	return atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&pq.unsafeData)),
		*((*unsafe.Pointer)(unsafe.Pointer(&old))),
		*((*unsafe.Pointer)(unsafe.Pointer(&new))),
	)
}

func (pq *preparedQuery) Close() error {
	data := pq.data()
	for len(data) > 0 {
		if pq.casData(data, emptyPQData) {
			break
		}
		data = pq.data()
	}
	errs := make([]error, 0, len(data))
	for _, v := range data {
		stmtData := v.stmtData()
		if stmtData != nil {
			if err := stmtData.stmt.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (pq *preparedQuery) executeQuery(ctx context.Context, q query) (_ stream.Stream, executeQueryErr error) {
	dbi, ok := DBInfoFromContext(ctx)
	if !ok {
		return nil, errDBInfoNotFoundInCtx
	}
	pqd, err := pq.getOrCreateData(ctx, dbi, q)
	if err != nil {
		return nil, fmt.Errorf(
			"getting or creating per-DB query data: %w",
			err,
		)
	}
	args, err := getOrAppendSQLArgs(ctx, nil, pqd.args)
	if err != nil {
		return nil, fmt.Errorf("appending arguments: %w", err)
	}
	rows, rowsCloser, err := pqd.executeQuery(ctx, q, args)
	if err != nil {
		return nil, fmt.Errorf(
			"executing per-DB query %v: %w",
			pqd, err,
		)
	}
	return createStreamFromRows(q, pqd.modelTypes, rows, rowsCloser)
}

func (pq *preparedQuery) getOrCreateData(ctx context.Context, dbi *DBInfo, q query) (*pqData, error) {
	pqd, ok := pq.data()[dbi]
	if !ok {
		sn, modelTypes, err := createSQLNodeFromQuery(ctx, dbi, q)
		if err != nil {
			// TODO: Sticky err on pqData?
			return nil, fmt.Errorf(
				"creating sql node from query %v: %w",
				q, err,
			)
		}
		sb := strings.Builder{}
		swr, err := dbi.MakeSQLWriter(ctx, &sb)
		if err != nil {
			return nil, fmt.Errorf(
				"creating SQLWriter: %w", err,
			)
		}
		_, err = swr.WriteSQL(ctx, sn)
		if err != nil {
			return nil, fmt.Errorf(
				"writing SQL %v with %v: %w",
				sn, swr, err,
			)
		}
		getOrAdded := [1]dbInfoPQData{
			{Key: dbi, Value: &pqData{
				query:          sb.String(),
				args:           swr.Args(),
				executionCount: 0,
				modelTypes:     modelTypes,
			}},
		}
		pq.getOrAddData(getOrAdded[:])
		pqd = getOrAdded[0].Value
	}
	return pqd, nil
}

type pqData struct {
	query          string
	unsafeStmtData *sqlStmtData
	args           []interface{}
	modelTypes     []*modelType
	executionCount int32
}

func (pqd *pqData) stmtData() *sqlStmtData {
	return (*sqlStmtData)(atomic.LoadPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&pqd.unsafeStmtData)),
	))
}

func (pqd *pqData) casStmtData(old, new *sqlStmtData) (swapped bool) {
	return atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&pqd.unsafeStmtData)),
		unsafe.Pointer(old),
		unsafe.Pointer(new),
	)
}

var errQuerierNotInContext = fmt.Errorf("SQL querier %w in context", expr.ErrNotFound)

func (pqd *pqData) executeQuery(
	ctx context.Context,
	q query,
	args []interface{},
) (
	rows sqlRows,
	closeRows func() error,
	err error,
) {
	closeRows = func() error { return nil }

	// pqd.stmtData is always prepared on a DB (database/sql
	// has some internal logic to try use the connection on which a
	// statement was prepared, if possible).
	//
	// We search the context for the most recently added:
	// 1. sqlStmter if the pqd.stmtData is not null, or
	// 2. sqlPreparer if the pqd.stmtData is null and
	//    db.shouldPrepare evaluates to true, else
	// 3. sqlQuerier
	stmtData := pqd.stmtData()
	if stmtData == nil {
		if db, ok := ctxutil.Value(
			ctx, (*DB)(nil),
		).(*DB); ok && db.shouldPreparer.shouldPrepare(
			ctx, q,
		) {
			sqlDB := db.SQLDB()
			stmt, err := sqlDB.PrepareContext(
				ctx, pqd.query,
			)
			if err != nil {
				return nil, closeRows, fmt.Errorf(
					"preparing query %v on %v: %w",
					pqd.query, sqlDB, err,
				)
			}
			newStmtData := &sqlStmtData{
				stmt:   stmt,
				preper: sqlDB,
			}
			if pqd.casStmtData(nil, newStmtData) {
				stmtData = newStmtData
			} else {
				stmtData = pqd.stmtData()
				if stmtData == nil {
					logger.Warn1(
						"Attempt to store prepared "+
							"query data: %#v "+
							"failed, yet the "+
							"actual data is nil.",
						newStmtData,
					)
					// use the statement we prepared
					// even though we couldn't
					// preserve it.
					stmtData = newStmtData
				}
			}
		}
	}
	closeStmt := func(stmt *sql.Stmt) func() error {
		closeRows2 := closeRows
		return func() error {
			err2 := closeRows2()
			err := stmt.Close()
			if err != nil {
				err = fmt.Errorf(
					"closing temporary statement %v: %w",
					stmt, err,
				)
			}
			if err2 == nil && err == nil {
				return nil
			}
			return errors.Join(err2, err)
		}
	}
	sentinelErr := func() error { return io.EOF }
	err = ctxutil.WalkValues(ctx, func(walkCtx context.Context, key, value interface{}) (walkErr error) {
		if stmtData != nil {
			switch key.(type) {
			case *sqlStmter:
				if v, ok := value.(sqlStmter); ok {
					stmt := v.StmtContext(ctx, stmtData.stmt)
					closeRows = closeStmt(stmt)
					rows, walkErr = stmt.QueryContext(ctx, args...)
					if walkErr == nil {
						walkErr = sentinelErr()
					}
					return
				}
			}
		}
		switch key.(type) {
		case *DB:
			if v, ok := value.(*DB); ok {
				value = v.SQLDB()
			}
		case *sqlQuerier:
		case *sql.DB:
		case *sql.Conn:
		case *sql.Tx:
		default:
			return nil
		}
		switch v := value.(type) {
		case sqlSQLQuerier:
			rows, walkErr = v.QueryContext(ctx, pqd.query, args...)
		case sqlQuerier:
			rows, walkErr = v.QueryContext(ctx, pqd.query, args...)
		default:
			return nil
		}
		if walkErr == nil {
			walkErr = sentinelErr()
		}
		return
	})
	if err == sentinelErr() {
		err = nil
	}
	if err != nil {
		return nil, closeRows, err
	}
	if rows == nil {
		return nil, func() error { return nil }, errQuerierNotInContext
	}
	return rows, closeRows, nil
}

type sqlStmtData struct {
	stmt   *sql.Stmt
	preper sqlPreparer
}

func createStreamFromRows(q query, modelTypes []*modelType, rows sqlRows, rowsCloser func() error) (stream.Stream, error) {
	columns := 0
	for _, mt := range modelTypes {
		columns += len(mt.columnNames)
	}
	return &rowsStream{
		query:      q,
		modelTypes: modelTypes,
		rows:       rows,
		values:     make([]interface{}, 0, columns),
		rowsCloser: rowsCloser,
	}, nil
}

type sqlNodeGenerator struct {
	dbi         *DBInfo
	q           query
	t           queryJoinTree
	sourceStack []*sqllang.Source

	modelTypes []*modelType
}

func (sng *sqlNodeGenerator) topSrc(indexFromTop int) *sqllang.Source {
	return sng.sourceStack[len(sng.sourceStack)-indexFromTop-1]
}

func createSQLNodeFromQuery(ctx context.Context, dbi *DBInfo, q query) (sn sqllang.Node, modelTypes []*modelType, err error) {
	sng := sqlNodeGenerator{
		dbi: dbi,
		q:   q,
		t: queryJoinTree{
			nodes:   make([]queryJoinTreeNode, 0, arbitraryCapacity),
			queries: make([]query, 0, arbitraryCapacity*arbitraryCapacity),
		},
		sourceStack: make([]*sqllang.Source, 1, arbitraryCapacity),
	}
	sng.t.init(q, 0, -1)
	src := &sqllang.Source{}
	sng.sourceStack[0] = src
	err = sng.initSQLSourceFromQueryTree(ctx, 0)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"initializing SQL source node %v: %w",
			sng, err,
		)
	}
	if src.Select != nil {
		return src.Select, sng.modelTypes, nil
	}
	return src, sng.modelTypes, nil
}

func (sng *sqlNodeGenerator) initSQLSourceFromQueryTree(ctx context.Context, queryJoinTreeIndex int) error {
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
	src := sng.topSrc(0)
	elem := sng.t.nodes[queryJoinTreeIndex]
	for _, q := range sng.t.queries[elem.firstChild : elem.firstChild+elem.children] {
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
				On: q.when,
			})
			joinSrc := &(*joins)[len(*joins)-1].Source
			sng.sourceStack = append(sng.sourceStack, joinSrc)
			if err := sng.initSQLSourceFromQueryTree(
				ctx, queryJoinTreeIndex+1,
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
			sng.modelTypes = append(sng.modelTypes, q.modelType)
			sqlTable := sng.dbi.sqlTableOf(q.modelType)
			if queryJoinTreeIndex == 0 || src.Select != nil {
				if err := initSQLSelectFromTableQuery(
					promoteToSelect(src), q,
					sqlTable,
				); err != nil {
					return err
				}
			} else if _, ok := sng.t.queries[elem.query].(*joinQuery); ok {
				if err := appendSQLSelectColumns(
					sng.sourceStack[0].Select, q,
					sqlTable,
				); err != nil {
					return err
				}
			} else {
				if err := initSQLSourceTableAndAliasFromTableQuery(
					src, q, sqlTable,
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func appendSQLSelectColumns(sel *sqllang.Select, q *tableQuery, sqlTable *sqlddl.Table) error {
	ddlCols := sqlTable.Columns
	t := reflect.New(q.modelType.unsafereflectType.ReflectType()).Interface()
	for i := range ddlCols {
		f := q.modelType.unsafereflectType.FieldPointer(t, i)
		sel.Columns = append(sel.Columns, sqllang.Column{
			Expr: expr.MemOf(q, t, f),
		})
	}
	return nil
}

func initSQLSelectFromTableQuery(sel *sqllang.Select, q *tableQuery, sqlTable *sqlddl.Table) error {
	if err := appendSQLSelectColumns(sel, q, sqlTable); err != nil {
		return fmt.Errorf("appending SQL Select from tableQuery: %w", err)
	}
	return initSQLSourceTableAndAliasFromTableQuery(&sel.From, q, sqlTable)
}

func initSQLSourceTableAndAliasFromTableQuery(src *sqllang.Source, q *tableQuery, sqlTable *sqlddl.Table) error {
	sb := strings.Builder{}
	sb.WriteByte('"')
	if sqlTable.TableName.SchemaName.Name != "" {
		sb.WriteString(sqlTable.TableName.SchemaName.Name)
	}
	sb.WriteString(sqlTable.TableName.Name)
	sb.WriteByte('"')
	src.Table = sb.String()
	src.Alias = q.Name()
	return nil
}

// queryJoinTree holds a flattened query tree:  Queries are linked lists
// of transformations eventually pointing to a `*tableQuery` at their
// root.  To generate the necessary SQL, we have to flip these around
// to traverse from the root to the leaves.
//
// We cannot just turn the pointers around because different queries
// can be built from the same root, so they are immutable.
type queryJoinTree struct {
	nodes   []queryJoinTreeNode
	queries []query
}

func (t *queryJoinTree) init(q query, queryIndex, parentIndex int) {
	start := len(t.queries)
	t.queries = appendQueryComponents(t.queries, q)
	end := len(t.queries)
	// swap the appended components so they go from inner to outer.
	// We need the nodes in this order for when we generate the
	// SQL statement.
	for i := range t.queries[start : start+((end-start)/2)] {
		t.queries[start+i], t.queries[end-1-i] = t.queries[end-1-i], t.queries[start+i]
	}
	t.nodes = append(t.nodes, queryJoinTreeNode{
		query:      queryIndex,
		parent:     parentIndex,
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
			t.init(jq.to, start+i, queryIndex)
		}
	}
}

type queryJoinTreeNode struct {
	// query is the index of the query object for this node
	// in the tree's nodes list
	query int

	// index of the parent of this join node.
	parent int

	// firstChild is the index of the first child of this query
	firstChild int

	// children is the
	children int
}

// tableQuery is a "root" query on a table
type tableQuery struct {
	modelType *modelType

	prepared preparedQuery

	// name is the name ("alias") of the query
	name atomicString
}

func newTableQuery(modelType *modelType) *tableQuery {
	tq := &tableQuery{
		modelType: modelType,
	}
	tq.prepared.init()
	return tq
}

func (q *tableQuery) preparedQuery() *preparedQuery { return &q.prepared }

var _ interface {
	query
	stream.Filterer
} = (*tableQuery)(nil)

func (q *tableQuery) Filter(ctx context.Context, e expr.Expr) stream.Streamer {
	return newFilterQuery(q, e)
}

func (q *tableQuery) Join(ctx context.Context, to stream.Streamer, when, then expr.Expr) stream.Streamer {
	return newJoinQueryOrLocal(ctx, q, to, when, then)
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
		rawName := q.modelType.rawName
		schemaName := rawName.SchemaName.Name
		if schemaName != "" {
			writeFirstRuneOfEachNamePart(&sb, schemaName)
		}
		writeFirstRuneOfEachNamePart(&sb, rawName.Name)
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
}

func newFilterQuery(from query, filter expr.Expr) *filterQuery {
	fq := &filterQuery{
		from:   from,
		filter: filter,
	}
	fq.prepared.init()
	return fq
}

var _ interface {
	query
	queryComponent
} = (*filterQuery)(nil)

func (q *filterQuery) Filter(ctx context.Context, e expr.Expr) stream.Streamer {
	return newFilterQuery(q.from, expr.And{q.filter, e})
}

func (q *filterQuery) Join(ctx context.Context, to stream.Streamer, when, then expr.Expr) stream.Streamer {
	return newJoinQueryOrLocal(ctx, q, to, when, then)
}

func (q *filterQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *filterQuery) query() query { return q.from }
func (q *filterQuery) Name() string { return q.from.Name() }
func (q *filterQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.prepared.executeQuery(ctx, q)
}
func (q *filterQuery) Var() expr.Var { return q.from.Var() }

type joinQuery struct {
	from     query
	prepared preparedQuery
	to       query
	when     expr.Expr
	then     expr.Expr
	name     atomicString
}

func newJoinQueryOrLocal(ctx context.Context, from query, to stream.Streamer, when, then expr.Expr) stream.Streamer {
	if toQ, ok := to.(query); ok {
		return newJoinQuery(from, toQ, when, then)
	}
	return stream.NewLocalJoiner(ctx, from, to, when, then)
}

func newJoinQuery(from, to query, when, then expr.Expr) *joinQuery {
	jq := &joinQuery{
		from: from,
		to:   to,
		when: when,
		then: then,
	}
	jq.prepared.init()
	return jq
}

var _ interface {
	query
	queryComponent
} = (*joinQuery)(nil)

func (q *joinQuery) preparedQuery() *preparedQuery { return &q.prepared }

func (q *joinQuery) query() query { return q.from }
func (q *joinQuery) Name() string {
	return q.name.LoadOrCreate(q, func(arg interface{}) string {
		q := arg.(*joinQuery)
		return q.from.Name() + q.to.Name()
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
	panic("TODO: handle projections")
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
	query      query
	modelTypes []*modelType
	rows       sqlRows
	values     []interface{}
	rowsCloser func() error
}

var _ interface {
	stream.Stream
	io.Closer
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
	t, ok := v.(expr.Tuple)
	if !ok {
		if len(rs.modelTypes) != 1 {
			return fmt.Errorf(
				"result set has a sequence of "+
					"model types, but bound model "+
					"%[1]v (type: %[1]T) is not a "+
					"tuple",
				v,
			)
		}
		t = expr.Tuple{v}
	}
	rs.values = rs.values[:0]
	for i, mt := range rs.modelTypes {
		rs.values = mt.AppendFieldPointers(rs.values, t[i])
	}
	if err = rs.rows.Scan(rs.values...); err != nil {
		return fmt.Errorf(
			"%v error scanning row: %w",
			rs, err,
		)
	}
	return nil
}

func (rs *rowsStream) Var() expr.Var { return rs.query.Var() }

func (rs *rowsStream) Close() error {
	errs := [...]error{
		rs.rows.Close(),
		rs.rowsCloser(),
	}
	return errors.Join(errs[:]...)
}
