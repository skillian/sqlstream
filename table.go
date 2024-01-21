package sqlstream

import (
	"reflect"
	"strings"
	"sync"

	"github.com/skillian/errors"
	"github.com/skillian/sqlstream/sqllang"
)

type Table interface {
	// Name gets the name of the table within the database (i.e.
	// with the database's naming convention applied).
	Name() string

	// Columns returns a slice of all of the defined table columns.
	// Do not modify the columns.
	Columns() []Column
}

// anyTable is the "base" non-generic table interface.
//
// Tables are database-specific objects.  The same struct model
// will have separate table implementations per database, based on the
// database's naming convention, RDBMS, and persistence-layer's
// columns' data types
type anyTable interface {
	Table

	// anyModelType gets the model type from which this table was
	// created.
	anyModelType() anyModelType
}

type tablesV1 struct {
	m       anyModelType
	tables  []anyTable
	columns []Column
	name    string
}

var multiTables = sync.Map{}

var _ interface {
	anyTable
} = (*tablesV1)(nil)

func (ts *tablesV1) anyModelType() anyModelType { return ts.m }
func (ts *tablesV1) Name() string               { return ts.name }
func (ts *tablesV1) Columns() []Column          { return ts.columns }

type tableV1[T any] struct {
	modelType    modelType
	tableColumns []Column
	name         string
}

func (t *tableV1[T]) Name() string               { return t.name }
func (t *tableV1[T]) anyModelType() anyModelType { return t.modelType }
func (t *tableV1[T]) Columns() []Column          { return t.tableColumns }

func (db *DBInfo) tableOf(modelType interface{}) table {
	mt := modelTypeOf(modelType)
	key := anyModelType(mt)
	t, loaded := db.tables.Load(key)
	if loaded {
		return t.(*tableV1[T])
	}
	newTable := func(db *DBInfo, mt modelType[T]) *tableV1[T] {
		tbl := &tableV1[T]{
			modelType: mt,
		}
		sb := strings.Builder{}
		if _, err := db.nameWritersTo.Table.WriteNameTo(&sb, mt.Name()); err != nil {
			panic(errors.ErrorfWithCause(
				err, "failed to write name to %T",
				&sb,
			))
		}
		tbl.name = sb.String()
		fts := mt.fieldV1s()
		tbl.tableColumns = make([]Column, len(fts))
		for i := range fts {
			sb.Reset()
			if _, err := db.nameWritersTo.Column.WriteNameTo(
				&sb, fts[i].name,
			); err != nil {
				panic(errors.ErrorfWithCause(
					err, "failed to write name to %T",
					&sb,
				))
			}
			tbl.tableColumns[i].Name = sb.String()
			// TODO: allow type to be determined from model's field tag
			tbl.tableColumns[i].SQLType = sqllang.TypeOf(
				reflect.Zero(fts[i].reflectType).Interface(),
			)
		}
		return tbl
	}
	tbl := newTable(db, mt)
	t, loaded = db.tables.LoadOrStore(key, tbl)
	if loaded {
		return t.(*tableV1[T])
	}
	return tbl
}

// Column description within a specific database
type Column struct {
	// Name of the column
	Name string

	// SQLType describes the type of the column
	SQLType sqllang.Type
}
