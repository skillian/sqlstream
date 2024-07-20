package sqlstream

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/skillian/expr/stream"
	"github.com/skillian/sqlstream/sqllang"
	sqlddl "github.com/skillian/sqlstream/sqllang/ddl"
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

type shouldPreparerFunc func(ctx context.Context, q query) bool

func (f shouldPreparerFunc) shouldPrepare(ctx context.Context, q query) bool { return f(ctx, q) }

type shouldPrepareAfterCount struct{ count int32 }

func (sp shouldPrepareAfterCount) shouldPrepare(ctx context.Context, q query) bool {
	if spr, ok := q.(interface{ preparedQuery() *preparedQuery }); ok {
		pq := spr.preparedQuery()
		// load w/o increment in so we don't roll over int32 in
		// long-running processes
		if atomic.LoadInt32(&pq.executionCount) >= sp.count {
			return false // already prepared
		}
		return atomic.AddInt32(&pq.executionCount, 1) == sp.count
	}
	return false
}

type DB struct {
	db             *sql.DB
	sqlWriterTo    SQLWriterTo
	nameWritersTo  NameWritersTo
	sqlTables      sync.Map // map[*modelType]*sqlddl.Table
	shouldPreparer shouldPreparer
}

type DBOption interface {
	applyOptionToDB(db *DB) error
}

type dbOptionFunc func(db *DB) error

func (f dbOptionFunc) applyOptionToDB(db *DB) error { return f(db) }

func NewDB(options ...DBOption) (*DB, error) {
	db := &DB{}
	for _, f := range options {
		if err := f.applyOptionToDB(db); err != nil {
			return nil, err
		}
	}
	// defaults:
	if db.nameWritersTo.Column == nil {
		db.nameWritersTo.Column = SnakeCaseLower
	}
	if db.nameWritersTo.Table == nil {
		db.nameWritersTo.Table = SnakeCaseLower
	}
	if db.nameWritersTo.Schema == nil {
		db.nameWritersTo.Schema = SnakeCaseLower
	}
	if db.shouldPreparer == nil {
		db.shouldPreparer = shouldPrepareAfterCount{count: 2}
	}
	return db, nil
}

// SQLDB gets the *sql.DB that this DB wraps,
func (db *DB) SQLDB() *sql.DB { return db.db }

// Query creates a query starting from the table associated with the
// given model
func (db *DB) Query(ctx context.Context, modelType interface{}) stream.Streamer {
	mt := modelTypeOf(modelType)
	tq := &tableQuery{
		table: table{
			db:        db,
			modelType: mt,
			sqlTable:  db.sqlTableOf(mt),
		},
	}
	return tq
}

func (db *DB) sqlTableOf(mt *modelType) *sqlddl.Table {
	createName := func(rawName string, nwt NameWriterTo, sqlName *string) {
		b := strings.Builder{}
		if *sqlName == "" {
			if _, err := nwt.WriteNameTo(&b, rawName); err != nil {
				panic(err)
			}
			*sqlName = b.String()
		}
		return
	}
	createTable := func(mt *modelType) *sqlddl.Table {
		rsfs := mt.ReflectStructFields()
		t := &sqlddl.Table{
			Columns:   make([]sqlddl.Column, len(rsfs)),
			TableName: mt.sqlName,
		}
		if t.TableName == (sqlddl.TableName{}) {
			createName(
				mt.rawName.SchemaName.Name,
				db.nameWritersTo.Schema,
				&t.TableName.SchemaName.Name,
			)
			createName(
				mt.rawName.Name,
				db.nameWritersTo.Table,
				&t.TableName.Name,
			)
		}
		type argType struct {
			db *DB
			t  *sqlddl.Table
		}
		if err := mt.iterFields(&argType{
			db: db,
			t:  t,
		}, func(f *modelTypeIterField) error {
			arg := f.arg.(*argType)
			c := &arg.t.Columns[f.index]
			*c = sqlddl.Column{
				ColumnName: sqlddl.ColumnName{
					TableName: t.TableName,
					Name:      f.sqlName,
				},
				Type: sqllang.TypeFromReflectType(f.reflectStructField.Type),
			}
			if c.ColumnName.Name == "" {
				createName(
					f.rawName,
					db.nameWritersTo.Column,
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
	if v, loaded := db.sqlTables.Load(key); loaded {
		return v.(*sqlddl.Table)
	}
	t := createTable(mt)
	if v, loaded := db.sqlTables.LoadOrStore(key, t); loaded {
		return v.(*sqlddl.Table)
	}
	return t
}
