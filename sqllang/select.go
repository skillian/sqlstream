package sqllang

import (
	"math/big"

	"github.com/skillian/expr"
)

var bigInts = func() (ints [11]big.Int) {
	for i := range ints {
		ints[i].SetInt64(int64(i))
	}
	return
}()

// Select is an abstract representation of a SELECT statement
type Select struct {
	Columns []Column
	From    Source
	Where   Where
	OrderBy []Sort
	Limit   *big.Int // Don't need to worry about int ranges 😆
}

func (*Select) sqllangNode() {}

func (sel *Select) SetLimitUint(i uint64) {
	if i < uint64(len(bigInts)) {
		sel.Limit = &bigInts[int(i)]
	}
	sel.Limit = (&big.Int{}).SetUint64(i)
}

type Column struct {
	Expr  expr.Expr
	Alias string
}

func (*Column) sqllangNode() {}

type Where struct {
	Expr expr.Expr
}

func (*Where) sqllangNode() {}
