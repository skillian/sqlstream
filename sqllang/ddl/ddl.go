package sqlddl

import (
	"strings"

	"github.com/skillian/sqlstream/sqllang"
)

type SchemaName struct {
	Name string
}

func (sn SchemaName) AppendTo(in []string) []string {
	if len(sn.Name) == 0 {
		return in
	}
	return append(in, sn.Name)
}

type Schema struct {
	Name SchemaName
}

type TableName struct {
	SchemaName
	Name string
}

func (tn TableName) AppendTo(in []string) []string {
	in = tn.SchemaName.AppendTo(in)
	if len(tn.Name) == 0 {
		return in
	}
	return append(in, tn.Name)
}

func (tn *TableName) Parse(name string) {
	prefix, suffix, ok := strings.Cut(name, ".") // dot separates schema from table name
	if ok {
		tn.SchemaName.Name = strings.TrimSpace(prefix)
		prefix = suffix
	}
	tn.Name = strings.TrimSpace(prefix)
}

type Table struct {
	TableName
	Columns     []Column
	Constraints *[]Constraint // pointer because we atomically swap it when we need to add FKs
}

type ColumnName struct {
	TableName
	Name string
}

func (cn ColumnName) AppendTo(in []string) []string {
	in = cn.TableName.AppendTo(in)
	return append(in, cn.Name)
}

func (cn *ColumnName) Parse(name string) {
	prefix, suffix, ok := cutLast(name, ".")
	if ok {
		cn.TableName.Parse(prefix)
		prefix = suffix
	}
	cn.Name = prefix
}

type Column struct {
	ColumnName
	Type sqllang.Type
}

type Constraint interface {
	constraint()
}

type UniqueConstraint []*Column

func (UniqueConstraint) constraint() {}

type PrimaryKeyConstraint []*Column

func (PrimaryKeyConstraint) constraint() {}

type ForeignKeyReference struct {
	Table   TableName
	Columns []*Column
}

type ForeignKeyConstraint struct {
	Columns    []*Column
	References ForeignKeyReference
}

func (ForeignKeyConstraint) constraint() {}

func cutLast(s, sep string) (before, after string, found bool) {
	if i := strings.LastIndex(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}
