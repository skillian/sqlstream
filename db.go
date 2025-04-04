package sqlstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/skillian/ctxutil"
	"github.com/skillian/expr"
	sqlddl "github.com/skillian/sqlstream/sqllang/ddl"
)

var (
	sqlScannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

type sqlExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type sqlPreparer interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

type sqlSQLQuerier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

type sqlQuerier interface {
	QueryContext(context.Context, string, ...interface{}) (sqlRows, error)
}

type sqlQuerierFunc func(context.Context, string, ...interface{}) (sqlRows, error)

func (f sqlQuerierFunc) QueryContext(ctx context.Context, s string, args ...interface{}) (sqlRows, error) {
	return f(ctx, s, args)
}

func sqlQuerierWrapRows(q sqlSQLQuerier) sqlQuerier {
	return sqlQuerierFunc(func(ctx context.Context, s string, args ...interface{}) (sqlRows, error) {
		rows, err := q.QueryContext(ctx, s, args)
		return rows, err
	})
}

type sqlRows interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(...interface{}) error
}

var _ sqlRows = (*sql.Rows)(nil)

type emptyRows struct{}

var _ sqlRows = emptyRows{}

func (emptyRows) Close() error                            { return nil }
func (emptyRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (emptyRows) Columns() ([]string, error)              { return nil, nil }
func (emptyRows) Err() error                              { return nil }
func (emptyRows) Next() bool                              { return false }
func (emptyRows) NextResultSet() bool                     { return false }
func (emptyRows) Scan(args ...interface{}) error          { return nil }

type sqlStmter interface {
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

type sqlTxer interface {
	BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error)
}

var ignoreErrorFunc = func(error) error { return nil }

func WithTransaction(ctx context.Context) (txCtx context.Context, endTx func(err error) error, err error) {
	txer, ok := ctxutil.Value(ctx, (*sqlTxer)(nil)).(sqlTxer)
	if !ok {
		return ctx, ignoreErrorFunc, fmt.Errorf("no %T in context", txer)
	}
	tx, err := txer.BeginTx(ctx, nil)
	if err != nil {
		return ctx, ignoreErrorFunc, fmt.Errorf("beginning transaction: %w", err)
	}
	return ctxutil.WithValue(ctx, (*sql.Tx)(nil), tx), func(err error) error {
		if err == nil {
			if err = tx.Commit(); err != nil {
				return fmt.Errorf(
					"committing transaction %v: %w",
					tx, err,
				)
			}
			return nil
		}
		if err2 := tx.Rollback(); err2 != nil {
			return errors.Join(
				err,
				fmt.Errorf(
					"rolling back transaction %v: %w",
					tx, err2,
				),
			)
		}
		logger.Info2(
			"rolled back transaction %v due to error: %v",
			tx, err,
		)
		return err
	}, nil
}

var _ = []interface {
	sqlExecer
	sqlPreparer
	sqlSQLQuerier
}{
	(*sql.DB)(nil),
	(*sql.Conn)(nil),
	(*sql.Tx)(nil),
}

// shouldPreparer checks if a query should be prepared
type shouldPreparer interface {
	// shouldPrepare should be called once when checking if a query
	// should be prepared.  It might mutate the query during this
	// evaluation (e.g. to increment a counter to indicate that
	// this is the 2nd, etc. time it's being run).
	shouldPrepare(context.Context, query) bool
}

type shouldPreparerFunc func(ctx context.Context, q query) bool

func (f shouldPreparerFunc) shouldPrepare(ctx context.Context, q query) bool { return f(ctx, q) }

// WithNeverPrepare configures a DB to never prepare its queries
func WithNeverPrepare() DBOption {
	return dbOptionFunc(func(db *DB) error {
		db.shouldPreparer = shouldPreparerFunc(func(ctx context.Context, q query) bool {
			return false
		})
		return nil
	})
}

type shouldPrepareAfterCount struct{ count int32 }

func (sp shouldPrepareAfterCount) shouldPrepare(ctx context.Context, q query) bool {
	if pqr, ok := q.(interface{ preparedQuery() *preparedQuery }); ok {
		pq := pqr.preparedQuery()
		// load w/o increment in so we don't roll over int32 in
		// long-running processes
		if atomic.LoadInt32(&pq.executionCount) >= sp.count {
			return false // already prepared
		}
		return atomic.AddInt32(&pq.executionCount, 1) == sp.count
	}
	return false
}

// WithQueryPrepareAfterCount configures the database to prepare
// its queries after they've been executed `count` times.
func WithQueryPrepareAfterCount(count int32) DBOption {
	return dbOptionFunc(func(db *DB) error {
		db.shouldPreparer = shouldPrepareAfterCount{count: count}
		return nil
	})
}

type DB struct {
	db             *sql.DB
	info           *DBInfo
	shouldPreparer shouldPreparer
}

type DBOption interface {
	applyOptionToDB(db *DB) error
}

type dbOptionFunc func(db *DB) error

func (f dbOptionFunc) applyOptionToDB(db *DB) error { return f(db) }

type DBInfoOption interface {
	applyOptionToDBInfo(dbi *DBInfo) error
}

type dbInfoOptionFunc func(dbi *DBInfo) error

func (f dbInfoOptionFunc) applyOptionToDBInfo(dbi *DBInfo) error { return f(dbi) }

type dbDBInfoOptionFunc dbInfoOptionFunc

func (f dbDBInfoOptionFunc) applyOptionToDB(db *DB) error          { return f(db.info) }
func (f dbDBInfoOptionFunc) applyOptionToDBInfo(dbi *DBInfo) error { return f(dbi) }

// WithSQLDB sets the DB's wrapped *sql.DB
func WithSQLDB(sqlDB *sql.DB) DBOption {
	return dbOptionFunc(func(db *DB) error {
		db.db = sqlDB
		return nil
	})
}

// WithSQLOpen passes its driverName and dataSourceName to sql.Open
// and wraps the returned *sql.DB
func WithSQLOpen(driverName, dataSourceName string) DBOption {
	return dbOptionFunc(func(db *DB) (err error) {
		db.db, err = sql.Open(driverName, dataSourceName)
		return
	})
}

func NewDB(options ...DBOption) (*DB, error) {
	db := &DB{}
	for _, f := range options {
		if err := f.applyOptionToDB(db); err != nil {
			return nil, err
		}
	}
	db.init()
	return db, nil
}

func (db *DB) init() {
	// defaults:
	if db.shouldPreparer == nil {
		db.shouldPreparer = shouldPrepareAfterCount{count: 2}
	}
}

// SQLDB gets the *sql.DB that this DB wraps,
func (db *DB) SQLDB() *sql.DB { return db.db }

type DBInfo struct {
	ArgWriterTo   ArgWriterTo
	ExprWriterTo  ExprWriterTo
	SQLWriterTo   SQLWriterTo
	NameWritersTo NameWritersTo
	sqlTables     sync.Map // map[*modelType]*sqlddl.Table
}

var (
	errDBInfoNotFound      = fmt.Errorf("*DBInfo %w", expr.ErrNotFound)
	errDBInfoNotFoundInCtx = fmt.Errorf("%w in context", errDBInfoNotFound)
)

func DBInfoFromContext(ctx context.Context) (dbi *DBInfo, ok bool) {
	dbi, ok = ctxutil.Value(ctx, (*DBInfo)(nil)).(*DBInfo)
	return
}

func WithDBInfo(dbi *DBInfo) DBOption {
	return dbOptionFunc(func(db *DB) error {
		db.info = dbi
		return nil
	})
}

func (dbi *DBInfo) AddToContext(ctx context.Context) context.Context {
	return ctxutil.WithValue(ctx, (*DBInfo)(nil), dbi)
}

func (dbi *DBInfo) getArgWriterTo() ArgWriterTo {
	if dbi.ArgWriterTo == nil {
		dbi.ArgWriterTo = odbcArgWriterTo{}
	}
	return dbi.ArgWriterTo
}

func (dbi *DBInfo) getExprWriterTo() ExprWriterTo {
	if dbi.ExprWriterTo == nil {
		dbi.ExprWriterTo = defaultExprWriterTo{}
	}
	return dbi.ExprWriterTo
}

func (dbi *DBInfo) getSQLWriterTo() SQLWriterTo {
	if dbi.SQLWriterTo == nil {
		dbi.SQLWriterTo = defaultSQLWriterTo{}
	}
	return dbi.SQLWriterTo
}

func (dbi *DBInfo) MakeSQLWriter(ctx context.Context, w io.Writer) (SQLWriter, error) {
	if sw, ok := w.(SQLWriter); ok {
		return sw, nil
	}
	sw := &sqlWriter{}
	sw.init(dbi, w)
	return sw, nil
}

func (dbi *DBInfo) sqlTableOf(mt *modelType) *sqlddl.Table {
	createName := func(rawName string, nwt *NameWriterTo, sqlName *string) {
		if *sqlName != "" {
			return
		}
		*sqlName = nameString(rawName, getNameWriterTo(nwt, SnakeCaseLower))
	}
	createTable := func(mt *modelType) *sqlddl.Table {
		t := &sqlddl.Table{
			Columns:   make([]sqlddl.Column, len(mt.structFields)),
			TableName: mt.sqlName,
		}
		if t.TableName == (sqlddl.TableName{}) {
			createName(
				mt.rawName.SchemaName.Name,
				&dbi.NameWritersTo.Schema,
				&t.TableName.SchemaName.Name,
			)
			createName(
				mt.rawName.Name,
				&dbi.NameWritersTo.Table,
				&t.TableName.Name,
			)
		}
		type argType struct {
			dbi *DBInfo
			t   *sqlddl.Table
		}
		if err := mt.iterFields(&argType{
			dbi: dbi,
			t:   t,
		}, func(f *modelTypeIterField) error {
			arg := f.arg.(*argType)
			c := &arg.t.Columns[f.index]
			*c = sqlddl.Column{
				ColumnName: sqlddl.ColumnName{
					TableName: t.TableName,
					Name:      f.sqlName,
				},
				Type: f.sqlType,
			}
			if c.ColumnName.Name == "" {
				createName(
					f.rawName,
					&dbi.NameWritersTo.Column,
					&c.ColumnName.Name,
				)
			}
			return nil
		}); err != nil {
			panic(err)
		}
		return t
	}
	key := interface{}(mt)
	if v, loaded := dbi.sqlTables.Load(key); loaded {
		return v.(*sqlddl.Table)
	}
	t := createTable(mt)
	if v, loaded := dbi.sqlTables.LoadOrStore(key, t); loaded {
		return v.(*sqlddl.Table)
	}
	return t
}
