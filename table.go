package sqlstream

import (
	"reflect"
	"strings"
	"time"

	"github.com/skillian/errors"
	"github.com/skillian/sqlstream/sqllang"
)

type table struct {
	modelType *modelType
	columns   []column
	name      string
}

func (db *DBInfo) tableOf(mt *modelType) *table {
	key := (interface{})(mt)
	t, loaded := db.tables.Load(key)
	if loaded {
		return t.(*table)
	}
	newTable := func(db *DBInfo, mt *modelType) *table {
		fts := mt.fields
		sb := strings.Builder{}
		if _, err := db.nameWritersTo.Table.WriteNameTo(&sb, mt.name); err != nil {
			panic(err)
		}
		tbl := &table{
			modelType: mt,
			columns:   make([]column, len(fts)),
			name:      sb.String(),
		}
		for i := range fts {
			sb.Reset()
			c := &tbl.columns[i]
			f := &fts[i]
			if _, err := db.nameWritersTo.Column.WriteNameTo(
				&sb, f.name,
			); err != nil {
				panic(errors.ErrorfWithCause(
					err, "failed to write name to %T",
					&sb,
				))
			}
			c.name = sb.String()
			// TODO: allow type to be determined from model's field tag
			c.sqlType = sqllang.TypeOf(
				reflect.Zero(f.structField.Type).Interface(),
			)
			if f.tag != (fieldTag{}) {
				nullableType, isNullable := c.sqlType.(sqllang.Nullable)
				if isNullable {
					c.sqlType = nullableType[0]
				}
				switch c.sqlType.(type) {
				case sqllang.Binary:
					c.sqlType = sqllang.Binary{
						Length: uint(f.tag.scale),
						Fixed:  f.tag.fixed,
					}
				case sqllang.Decimal:
					c.sqlType = sqllang.Decimal{
						Scale:     uint(f.tag.scale),
						Precision: uint(f.tag.prec),
					}
				case sqllang.Float:
					c.sqlType = sqllang.Float{
						Mantissa: uint(f.tag.scale),
					}
				case sqllang.String:
					c.sqlType = sqllang.String{
						Length: uint(f.tag.scale),
						Fixed:  f.tag.fixed,
					}
				case sqllang.Time:
					c.sqlType = sqllang.Time{
						Prec: time.Duration(f.tag.prec),
					}
				}
				if isNullable {
					nullableType[0] = c.sqlType
					c.sqlType = nullableType
				}
			}
		}
		return tbl
	}
	tbl := newTable(db, mt)
	t, loaded = db.tables.LoadOrStore(key, tbl)
	if loaded {
		return t.(*table)
	}
	return tbl
}

// Column description within a specific database
type column struct {
	// Name of the column
	name string

	// SQLType describes the type of the column
	sqlType sqllang.Type
}
