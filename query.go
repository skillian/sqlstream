package sqlstream

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
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
	stmt           *sql.Stmt
	args           []interface{}
	preparer       sqlPreparer
	executionCount int32
}

func (pq *preparedQuery) executeQuery(ctx context.Context, q query, f func(*sql.Stmt, context.Context, ...interface{}) (*sql.Rows, error)) (stream.Stream, error) {
	args, err := func() (args []interface{}, err error) {
		var vs expr.Values
		for i, arg := range pq.args {
			if va, ok := arg.(expr.Var); ok {
				if args == nil {
					args = make([]interface{}, len(pq.args))
					copy(args, pq.args[:i])
				}
				if vs == nil {
					vs, err = expr.ValuesFromContext(ctx)
					if err != nil {
						return
					}
				}
				args[i], err = vs.Get(ctx, va)
				if err != nil {
					return
				}
				continue
			}
			if args != nil {
				args[i] = arg
			}
		}
		if args == nil {
			args = pq.args
		}
		return
	}()
	if err != nil {
		return nil, err
	}
	var tq *tableQuery
	preparer, ok := ctxutil.Value(ctx, (*sqlPreparer)(nil)).(sqlPreparer)
	if !ok {
		if tq == nil {
			tq, _ = tableQueryOf(q)
		}
		preparer = tq.table.db.db
	}
	stmt := (*sql.Stmt)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pq.stmt))))
	if stmt != nil {
		if pq.preparer != preparer {
			if stmter, ok := preparer.(sqlStmter); ok {
				stmt = stmter.StmtContext(ctx, stmt)
			}
		}
		rows, err := f(stmt, ctx, args)
		if err != nil {
			return nil, err
		}
		return pq.createStreamFromRows(rows)
	}
	if tq == nil {
		tq, _ = tableQueryOf(q)
	}
	sn, err := pq.createSQLNodeFromQuery(ctx, q)
	sb := strings.Builder{}
	sw := smallWriterCounterOf(&sb)
	sn, err := tq.table.db.sqlWriterTo.WriteSQLTo(ctx, sw, sn)

	if atomic.AddInt32(&pq.prepareCountdown, -1) > 0 {
		stmt, err = preparer.PrepareContext(ctx)
	}
}

func (pq *preparedQuery) createSQLNodeFromQuery(ctx context.Context, q query) (sn sqllang.Node, err error) {
	// get the query component chain into a slice and then iterate
	// through it backwards (i.e. from the root *tableQuery to the
	// last component) so that we can easily build the Select.
	components := func() (components []query) {
		components = make([]query, 0, arbitraryCapacity)
		for {
			components = append(components, q)
			qc, ok := q.(queryComponent)
			if !ok {
				break
			}
			q = qc.query()
		}
		return
	}()
	sel := &sqllang.Select{}
	for i := range components {
		switch q := components[len(components)-i-1].(type) {
		case *filterQuery:
			if sel.Where.Expr == nil {
				sel.Where.Expr = q.filter
			} else {
				sel.Where.Expr = expr.And{
					sel.Where.Expr,
					q.filter,
				}
			}
		case *joinQuery:
			sel.From.Joins = append(sel.From.Joins, sqllang.Join{})
		case *mapQuery:
			panic("// TODO: implement *mapQuery")
		case *tableQuery:

		}
	}

	sn = sel
}

func (pq *preparedQuery) createStreamFromRows(rows *sql.Rows) (stream.Stream, error) {

}

// tableQuery is a "root" query on a table
type tableQuery struct {
	table table

	prepared preparedQuery

	// name is the name ("alias") of the query
	name string
}

func (q *tableQuery) preparedQuery() *preparedQuery { return &q.prepared }

var _ interface {
	query
} = (*tableQuery)(nil)

func (q *tableQuery) Name() string { return q.name }

func (q *tableQuery) Stream(ctx context.Context) (stream.Stream, error) {
	stmt := (*sql.Stmt)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&q.lazy.prepared))))
	if stmt != nil {
		preparer, ok := ctxutil.Value(ctx, (*sqlPreparer)(nil)).(sqlPreparer)
		if !ok {
			preparer = q.table.db.db
		}
		if preparer != q.lazy.preparer {

		}
	}
	execer, ok := ctxutil.Value(ctx, (*sqlExecer)(nil)).(sqlExecer)
	if !ok {
		execer = q.table.db.db
	}
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
	return q.prepared.executeQuery(ctx, q, (*sql.Stmt).QueryContext)
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
	return q.prepared.executeQuery(ctx, q, (*sql.Stmt).QueryContext)
}
func (q *joinQuery) Var() expr.Var { return q.Var() }

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
	return q.prepared.executeQuery(ctx, q, (*sql.Stmt).QueryContext)
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
	return q.prepared.executeQuery(ctx, q, (*sql.Stmt).QueryContext)
}
func (q *sortQuery) Var() expr.Var { return q.from.Var() }

func initSelect(sel *sqllang.Select, q query) error {
	components := queryComponents(q)
	for i := range components {
		switch q := components[len(components)-i-1].(type) {
		case *filterQuery:
			if sel.Where.Expr == nil {
				sel.Where.Expr = q.filter
			} else {
				sel.Where.Expr = expr.And{
					sel.Where.Expr,
					q.filter,
				}
			}
		case *joinQuery:
			sel.From.Joins = append(sel.From.Joins, sqllang.Join{
				Source: sqllang.Source{
					Select: &sqllang.Select{},
				},
			})
		case *mapQuery:
			panic("// TODO: implement *mapQuery")
		case *tableQuery:

		}
	}
}

func queryComponents(q query) (components []query) {
	components = make([]query, 0, arbitraryCapacity)
	for {
		components = append(components, q)
		qc, ok := q.(queryComponent)
		if !ok {
			break
		}
		q = qc.query()
	}
	return
}
