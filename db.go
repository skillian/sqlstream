package sqlstream

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"math/bits"
	"reflect"
	"sync"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/sqlstream/sqllang"
)

var (
	// errDBMismatch is returned when you try to join queries
	// created from separate databases.
	//
	// TODO: Maybe this shouldn't be an error and should just be
	// joined locally?
	errDBMismatch = errors.New("mismatched *DBs")
)

// DBInfo contains database information without a *sql.DB
type DBInfo struct {
	driverDialect DriverDialect
	tables        sync.Map // modelType -> table
	nameWritersTo NameWritersTo
}

type dbAndInfoOptionFunc func(dbi *DBInfo) error

func (f dbAndInfoOptionFunc) applyToDBInfo(dbi *DBInfo) error { return f(dbi) }
func (f dbAndInfoOptionFunc) applyToDB(db *DB) error          { return f(&db.info) }

func WithDriverDialect(dd DriverDialect) interface {
	DBInfoOption
	DBOption
} {
	return dbAndInfoOptionFunc(func(dbi *DBInfo) error {
		if dbi.driverDialect != nil {
			logger.Warn4(
				"redefinition of %v.%v from %#v to %#v",
				dbi, expr.ReflectStructFieldOf(
					dbi, &dbi.driverDialect,
				).Name, dbi.driverDialect, dd,
			)
		}
		dbi.driverDialect = dd
		return nil
	})
}

// DB wraps a `*sql.DB` to provide query building functionality
type DB struct {
	db   *sql.DB
	info DBInfo
}

func NewDB(options ...DBOption) (db *DB, err error) {
	db = &DB{}
	for _, option := range options {
		if err = option.applyToDB(db); err != nil {
			return nil, err
		}
	}
	if db.info.nameWritersTo.Column == nil {
		db.info.nameWritersTo.Column = SnakeCaseLower
	}
	if db.info.nameWritersTo.Table == nil {
		db.info.nameWritersTo.Table = SnakeCaseLower
	}
	if db.info.nameWritersTo.Schema == nil {
		db.info.nameWritersTo.Schema = SnakeCaseLower
	}
	return
}

// DB returns the wrapped `*sql.DB` so it can be used manually or from
// another library
func (db *DB) DB() *sql.DB { return db.db }

func (db *DB) Query(ctx context.Context, modelType interface{}) stream.Streamer {
	mt := modelTypeOf(modelType)
	return &tableQuery{
		db:    db,
		table: db.info.tableOf(mt),
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
	case *tableQuery:
		return db.tableQueryStreamer(ctx, queryStack)
	}
	return errorStreamer{err: errors.Errorf(
		"unknown root query %[1]v (type: %[1]T)", q,
	)}
}

func (db *DB) tableQueryStreamer(ctx context.Context, queryStack []query) stream.Streamer {
	// tableAliasOf borrows space in the buffer to write an
	// alias into it, read it out, and truncate the alias back out.
	tableAliasOf := func(sw SQLWriter, buf *bytes.Buffer, q query) (string, error) {
		i64, err := sw.WriteExpr(ctx, q)
		if err != nil {
			return "", errorStreamer{err}
		}
		indexOfAlias := buf.Len() - int(i64)
		result := string(buf.Bytes()[indexOfAlias:])
		buf.Truncate(indexOfAlias)
		return result, nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	sw, err := db.info.driverDialect.SQLWriter(ctx, buf)
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
		case *tableQuery:
			sel.From.Table = q.table.name
			sel.From.Alias, err = tableAliasOf(sw, buf, q)
			if selectExpr == nil {
				selectExpr = q.Var()
			}
		case *filterQuery:
			if sel.Where.Expr == nil {
				sel.Where.Expr = q.where
			} else {
				sel.Where.Expr = expr.And{sel.Where.Expr, q.where}
			}
		case *joinQuery:
			tq := tableQueryOf(q)
			alias, err := tableAliasOf(sw, buf, tq)
			if err != nil {
				return errorStreamer{err}
			}
			sel.From.Joins = append(sel.From.Joins, sqllang.Join{
				Source: sqllang.Source{
					Table: tq.table.name,
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
		case *tableQuery:
			mt := e.table.modelType
			fs := mt.fields
			if cap(cols)-len(cols) < len(fs) {
				cols2 := make([]sqllang.Column, len(cols), 1<<bits.Len(uint(len(cols)+len(fs))))
				copy(cols2, cols)
				cols = cols2
			}
			ev := e.Var()
			for _, f := range fs {
				cols = append(cols, sqllang.Column{
					Expr: expr.Mem{ev, f.structField},
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
	if _, err = sw.WriteSQL(ctx, &sel); err != nil {
		return errorStreamer{errors.ErrorfWithCause(
			err, "error during SQL generation from %#v",
			&sel,
		)}
	}
	sqlString := buf.String()
	logger.Debug1("SQL:\n\t%s", sqlString)
	stmt, err := db.db.PrepareContext(ctx, sqlString)
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
	tq := tableQueryOf(sr.source)
	return &sqlRowsStream{
		sr:       sr,
		rows:     rows,
		scanArgs: make([]interface{}, len(tq.table.columns)),
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
