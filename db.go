package sqlstream

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/skillian/expr/stream"
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

type DB struct {
	db               *sql.DB
	sqlWriterTo      SQLWriterTo
	nameWritersTo    NameWritersTo
	sqlTables        sync.Map // map[*modelType]*sqlddl.Table
	prepareCountdown int32
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
	if db.prepareCountdown == 0 {
		// 2:  First time, do not prepare, just execute the
		// query.  Second time, prepare the query and execute
		// it because it might be run a 3rd or more times.
		db.prepareCountdown = 2
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
	createName := func(mt *modelType) (tn sqlddl.TableName) {
		type selectorNameWriterTo struct {
			fn  func(*sqlddl.TableName) *string
			nwt NameWriterTo
		}
		b := bytes.Buffer{}
		for _, snwt := range []selectorNameWriterTo{
			selectorNameWriterTo{
				func(x *sqlddl.TableName) *string { return &x.SchemaName.Name },
				db.nameWritersTo.Schema,
			},
			selectorNameWriterTo{
				func(x *sqlddl.TableName) *string { return &x.Name },
				db.nameWritersTo.Table,
			},
		} {
			dest := snwt.fn(&tn)
			if *dest == "" {
				if _, err := snwt.nwt.WriteNameTo(&b, *snwt.fn(&mt.rawName)); err != nil {
					panic(err)
				}
				*dest = b.String()
				b.Reset()
			}
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
			t.TableName = createName(mt)
		}
		b := bytes.Buffer{}
		for i := range rsfs {
			rsf := &rsfs[i]
			t.Columns[i] = sqlddl.Column{
				ColumnName: sqlddl.ColumnName{
					TableName: t.TableName,
				},
			}
			if tag, ok := rsf.Tag.Lookup(tagName); ok {
				name, tag, ok := strings.Cut(tag)
			}
		}
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
