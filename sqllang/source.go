package sqllang

import (
	"github.com/skillian/expr"
)

// Ident is a SQL identifier
type Ident string

func IdentFromString(id string) Ident { return Ident(id) }

func (id Ident) String() string { return string(id) }

// Source is a source of column data.
type Source struct {
	// Table is the name of a table/view in the database
	Table string

	// Alias is an alias of the table/view.  It can be used with
	// the Table or Select or by itself if the alias is defined
	// elsewhere.
	Alias string

	// Joins holds zero or more INNER JOINs to other sources.
	Joins []Join

	// Select is an optional subquery that can be used instead
	// of Table.
	Select *Select
}

func (*Source) sqllangNode() {}

// Join represents an INNER JOIN into another source.
type Join struct {
	Source Source
	On     expr.Expr
}

func (*Join) sqllangNode() {}

// Sort results
type Sort struct {
	By   expr.Expr
	Desc bool
}

func (*Sort) sqllangNode() {}
