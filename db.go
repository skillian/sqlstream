package sqlstream

import (
	"context"
	"database/sql"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/skillian/expr/stream"
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

type sqlQuerier interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

type sqlStmter interface {
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

type sqlTxer interface {
	BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error)
}

var _ = []interface {
	sqlExecer
	sqlPreparer
	sqlQuerier
}{
	(*sql.DB)(nil),
	(*sql.Conn)(nil),
	(*sql.Tx)(nil),
}

type shouldPreparer interface {
	shouldPrepare(context.Context, query) bool
}

//type shouldPreparerFunc func(ctx context.Context, q query) bool

//func (f shouldPreparerFunc) shouldPrepare(ctx context.Context, q query) bool { return f(ctx, q) }

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
	info           DBInfo
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

func (f dbDBInfoOptionFunc) applyOptionToDB(db *DB) error          { return f(&db.info) }
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
	db.info.Init()
	// defaults:
	if db.shouldPreparer == nil {
		db.shouldPreparer = shouldPrepareAfterCount{count: 2}
	}
}

// Query creates a query starting from the table associated with the
// given model
func (db *DB) Query(ctx context.Context, modelType interface{}) stream.Streamer {
	mt := modelTypeOf(modelType)
	tq := &tableQuery{
		table: table{
			db:        db,
			modelType: mt,
			sqlTable:  db.info.sqlTableOf(mt),
		},
	}
	return tq
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

func (dbi *DBInfo) Init() {
	if dbi.ArgWriterTo == nil {
		dbi.ArgWriterTo = odbcArgWriterTo{}
	}
	if dbi.ExprWriterTo == nil {
		dbi.ExprWriterTo = defaultExprWriterTo{}
	}
	if dbi.SQLWriterTo == nil {
		dbi.SQLWriterTo = defaultSQLWriterTo{}
	}
	dbi.NameWritersTo.init()
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
	createName := func(rawName string, nwt NameWriterTo, sqlName *string) {
		if *sqlName != "" {
			return
		}
		*sqlName = nameString(rawName, nwt)
	}
	createTable := func(mt *modelType) *sqlddl.Table {
		t := &sqlddl.Table{
			Columns:   make([]sqlddl.Column, len(mt.structFields)),
			TableName: mt.sqlName,
		}
		if t.TableName == (sqlddl.TableName{}) {
			createName(
				mt.rawName.SchemaName.Name,
				dbi.NameWritersTo.Schema,
				&t.TableName.SchemaName.Name,
			)
			createName(
				mt.rawName.Name,
				dbi.NameWritersTo.Table,
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
					dbi.NameWritersTo.Column,
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
