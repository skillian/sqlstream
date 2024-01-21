package sqlstream

import (
	"context"
	"database/sql"
	"io"
	"math/bits"
	"reflect"
	"strings"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/sqlstream/sqllang"
	"github.com/skillian/syng"
)

var (
	errDBMismatch = errors.New("mismatched *DBs")
)

// DBInfo contains database information without a *sql.DB
type DBInfo struct {
	driverDialect DriverDialect
	tables        syng.Map[anyModelType, anyTable]
	nameWritersTo NameWritersTo
}

type DBInfoOption interface {
	ApplyDBInfoOption(dbi *DBInfo)
}

type dbAndInfoOptionFunc func(dbi *DBInfo)

func (f dbAndInfoOptionFunc) ApplyDBInfoOption(dbi *DBInfo) { f(dbi) }
func (f dbAndInfoOptionFunc) ApplyDBOption(db *DB)          { f(&db.info) }

func WithDriverDialect(dd DriverDialect) interface {
	DBInfoOption
	DBOption
} {
	return dbAndInfoOptionFunc(func(dbi *DBInfo) {
		dbi.driverDialect = dd
	})
}

type DBOption interface {
	ApplyDBOption(db *DB)
}

// DB wraps a `*sql.DB` to provide query building functionality
type DB struct {
	db   *sql.DB
	info DBInfo
}

func NewDB(options ...DBOption) *DB {
	db := &DB{}
	for _, option := range options {
		option.ApplyDBOption(db)
	}
	return db
}

// DB returns the wrapped `*sql.DB` so it can be used manually or from
// another library
func (db *DB) DB() *sql.DB { return db.db }

func (db *DB) Query(ctx context.Context) stream.Streamer {
	return &tableQueryV1{
		db:    db,
		table: tableV1Of(&db.info),
	}
}

// queryStreamer is the entry point called by stream.Streamer
// implementations within this package to create a stream.Streamer
// implementation from results from this database.
func (db *DB) queryStreamer(ctx context.Context, q query) stream.Streamer {
	queryStack := make([]query, 0, arbitraryCapacity)
	for q := q; q != nil; q = q.source() {
		queryStack = append(queryStack, q)
	}
	switch queryStack[len(queryStack)-1].(type) {
	case *tableQueryV1:
		return db.queryStreamerV1(ctx, queryStack)
	}
	return errorStreamer{err: errors.Errorf(
		"unknown root query %[1]v (type: %[1]T)", q,
	)}
}

func (db *DB) queryStreamerV1(ctx context.Context, queryStack []query) stream.Streamer {
	sw, err := db.info.driverDialect.SQLWriterTo(ctx)
	if err != nil {
		return errorStreamer{errors.ErrorfWithCause(
			err, "failed to create %v from %v",
			reflect.TypeOf(&sw).Elem().Name(),
			db.info.driverDialect,
		)}
	}
	sel := sqllang.Select{}
	selectExpr := expr.Expr(nil)
	for i := len(queryStack) - 1; i >= 0; i-- {
		switch q := queryStack[i].(type) {
		case *tableQueryV1:
			sel.From.Table = q.table.Name()
			sel.From.Alias, err = tableAliasOf(ctx, sw, q)
			if err != nil {
				return errorStreamer{err}
			}
			if selectExpr == nil {
				selectExpr = q.Var()
			}
		case *filterQueryV1:
			if sel.Where.Expr == nil {
				sel.Where.Expr = q.where
			} else {
				sel.Where.Expr = expr.And{sel.Where.Expr, q.where}
			}
		case *joinQueryV1:
			t, tq := tableOfQuery(q)
			alias, err := tableAliasOf(ctx, sw, tq)
			if err != nil {
				return errorStreamer{err}
			}
			sel.From.Joins = append(sel.From.Joins, sqllang.Join{
				Source: sqllang.Source{
					Table: t.Name(),
					Alias: alias,
				},
				On: q.on,
			})
			if selectExpr == nil {
				selectExpr = q.proj
			}
		default:
			return errorStreamer{errors.Errorf(
				"unknown v1 query %[1]v (type: %[2]T)",
				q,
			)}
		}
	}
	var unpackColumns func(cols []sqllang.Column, e expr.Expr) []sqllang.Column
	unpackColumns = func(cols []sqllang.Column, e expr.Expr) []sqllang.Column {
		switch e := e.(type) {
		case expr.Tuple:
			for _, x := range e {
				cols = unpackColumns(cols, x)
			}
		case *tableQueryV1:
			mt := e.table.anyModelType()
			fs := mt.anyFields()
			if cap(cols)-len(cols) < len(fs) {
				cols2 := make([]sqllang.Column, len(cols), 1<<bits.Len(uint(len(cols)+len(fs))))
				copy(cols2, cols)
				cols = cols2
			}
			m := newAny(mt)
			ev := e.Var()
			for _, f := range fs {
				cols = append(cols, sqllang.Column{
					Expr: expr.MemOf(ev, m, f.pointerToAny(m)),
				})
			}

		default:
			cols = append(cols, sqllang.Column{
				Expr: e,
			})
		}
		return cols
	}
	sel.Columns = unpackColumns(nil, selectExpr)
	sb := strings.Builder{}
	if _, err = sw.WriteSQLTo(ctx, &sb, &sel); err != nil {
		return errorStreamer{errors.ErrorfWithCause(
			err, "error during SQL generation from %#v",
			&sel,
		)}
	}
	stmt, err := db.db.PrepareContext(ctx, sb.String())
	if err != nil {
		return errorStreamer{errors.ErrorfWithCause(
			err, "error preparing statement",
		)}
	}
	return &sqlStmtStreamer{
		source: queryStack[0],
		stmt:   stmt,
		args:   sw.Args(),
	}
}

type errorStreamer struct {
	err error
}

func (err errorStreamer) Stream(ctx context.Context) (stream.Stream, error) {
	return nil, err.err
}

func (err errorStreamer) Var() expr.Var { return nil }

func (err errorStreamer) Error() string { return err.err.Error() }

type sqlStmtStreamer struct {
	source query
	stmt   *sql.Stmt
	args   []interface{}
}

var _ interface {
	stream.Streamer
} = (*sqlStmtStreamer)(nil)

func (sr *sqlStmtStreamer) Stream(ctx context.Context) (st stream.Stream, err error) {
	args, err := replaceArgs(ctx, sr.args)
	if err != nil {
		return nil, errors.ErrorfWithCause(
			err,
			"error replacing statement streamer's args",
		)
	}
	rows, err := sr.stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, errors.ErrorfWithCause(
			err, "error executing (%T).QueryContext",
			rows,
		)
	}
	t, _ := tableOfQuery(sr.source)
	return &sqlRowsStream{
		sr:       sr,
		rows:     rows,
		scanArgs: make([]interface{}, len(t.anyModelType().anyFields())),
	}, nil
}

func (sr *sqlStmtStreamer) Var() expr.Var { return sr.source.Var() }

type sqlRowsStream struct {
	sr       *sqlStmtStreamer
	rows     *sql.Rows
	scanArgs []interface{}
}

func (st *sqlRowsStream) Next(ctx context.Context) error {
	if !st.rows.Next() {
		return io.EOF
	}
	vs, err := expr.ValuesFromContext(ctx)
	if err != nil {
		return err
	}
	mv, err := vs.Get(ctx, st.Var())
	if err != nil {
		return err
	}
	fa, ok := mv.(FieldsAppender)
	if !ok {
		return errors.Errorf(
			"%v var (%v) must have %v associated with it, "+
				"not %[4]v (type: %[4]T)",
			st, st.Var(), reflect.TypeOf(&fa).Elem().Name(),
			mv,
		)
	}
	st.scanArgs = fa.AppendFields(st.scanArgs[:0])
	if err = st.rows.Scan(st.scanArgs...); err != nil {
		return errors.ErrorfWithCause(
			err, "error scanning model %v fields into %#v",
			fa, st.scanArgs,
		)
	}
	return nil
}

func (st *sqlRowsStream) Var() expr.Var { return st.sr.Var() }
