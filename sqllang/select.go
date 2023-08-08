package sqllang

import (
	"math/big"

	"github.com/skillian/expr"
)

// Select is an abstract representation of a SELECT statement
type Select struct {
	Columns []expr.Expr
	From    Source
	Where   expr.Expr
	OrderBy []Sort
	Limit   *big.Int // Don't need to worry about int ranges 😆
}

type WalkFunc func(node interface{}) error

// Walk walks through the SQL language nodes to apply transformations
func Walk(node interface{}, f WalkFunc) (err error) {
	if err = f(node); err != nil {
		return
	}
	switch node := node.(type) {
	case *Select:
		if err = Walk(&node.From, f); err != nil {
			return
		}
		for i := range node.OrderBy {
			if err = Walk(&node.OrderBy[i], f); err != nil {
				return
			}
		}
	case *Source:
		if node.Select != nil {
			if err = Walk(node.Select, f); err != nil {
				return
			}
		}
		for i := range node.Joins {
			if err = Walk(&node.Joins[i], f); err != nil {
				return
			}
		}
	case *Join:
		if err = Walk(&node.Source, f); err != nil {
			return
		}
		if err = Walk(node.On, f); err != nil {
			return
		}
	case *Sort:
		if err = Walk(node.By, f); err != nil {
			return
		}
	}
	return f(nil)
}
