package sqllang

import (
	"math/big"

	"github.com/skillian/expr"
)

// Select is an abstract representation of a SELECT statement
type Select struct {
	Columns []Column
	From    Source
	Where   Where
	OrderBy []Sort
	Limit   *big.Int // Don't need to worry about int ranges 😆
}

func (*Select) sqllangNode() {}

type Column struct {
	Expr  expr.Expr
	Alias string
}

func (*Column) sqllangNode() {}

type Where struct {
	Expr expr.Expr
}

func (*Where) sqllangNode() {}
