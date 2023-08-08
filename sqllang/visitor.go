package sqllang

import (
	"fmt"

	"github.com/skillian/expr"
)

// Node is a SQL language node to be visited
type Node interface{}

// Visitor is a value that can implement some sqllang node visiting
// functions.  Inspect the implementation of Visit to see the kinds of
// methods that are checked.
type Visitor interface{}

// VisitContext is optional context information that can be checked from
// any of the Visitor implementations
type VisitContext interface{}

var (
	// ColumnsField
	ColumnsField interface{} = func() interface{} {
		var s Select
		return expr.MemOf(nil, &s, &s.Columns)[1]
	}()
)

// Visit a node with a visitor.  If the visitor does not support the
// node type, the node is returned directly.
func Visit(n Node, v Visitor) (out interface{}, err error) {
	var visit func(context interface{}, n Node, v Visitor) (out interface{}, err error, ok bool)
	var visitSelect func(context interface{}, s *Select, v Visitor) (out interface{}, err error, ok bool)
	var visitColumn func(context interface{}, e expr.Expr, v Visitor) (out interface{}, err error, ok bool)
	visit = func(context interface{}, n Node, v Visitor) (out interface{}, err error, ok bool) {
		switch v := v.(type) {
		case interface {
			Visit(interface{}, Node, Visitor) (interface{}, error)
		}:
			out, err = v.Visit(context, n, v)
			ok = true
			return
		case interface {
			Visit(Node, Visitor) (interface{}, error)
		}:
			out, err = v.Visit(n, v)
			ok = true
			return
		}
		return nil, nil, false
	}
	visitColumn = func(context interface{}, e Expr, v Visitor) (out interface{}, err error, ok bool) {
		switch v := v.(type) {
		case interface {
			VisitColumn(Expr, Visitor) (Expr, error)
		}:
			out, err = v.VisitColumn(e, v)
			ok = true
			return
		}
		return visit(context, e, v)
	}
	visitSelect = func(context interface{}, n *Select, v Visitor) (out interface{}, err error, ok bool) {
		switch v := v.(type) {
		case interface {
			VisitSelect(interface{}, *Select, Visitor) (*Select, error)
		}:
			out, err = v.VisitSelect(context, n, v)
			ok = true
			return
		case interface {
			VisitSelect(interface{}, *Select, Visitor) (interface{}, error)
		}:
			out, err = v.VisitSelect(context, n, v)
			ok = true
			return
		case interface {
			VisitSelect(*Select, Visitor) (*Select, error)
		}:
			out, err = v.VisitSelect(n, v)
			ok = true
			return
		case interface {
			VisitSelect(*Select, Visitor) (interface{}, error)
		}:
			out, err = v.VisitSelect(s, v)
			ok = true
			return
		}
		out, err, ok = visit(context, n, v)
		if ok {
			return
		}
		for i, c := range n.Columns {
			out, err, ok = visitColumn()
		}
	}
	type JoinVisitor interface {
		VisitJoin(*Join, Visitor) (interface{}, error)
	}
	switch n := n.(type) {
	case *Select:
		if v, ok := v.(interface {
			VisitSelect(*Select, Visitor) (interface{}, error)
		}); ok {
			return v.VisitSelect(n, v)
		}
		for i, c := range n.Columns {
			if n.Columns[i], err = Visit(c, v); err != nil {
				return nil, err
			}
		}
		x, err := Visit(&n.From, v)
		if err != nil {
			return nil, err
		}
		s, ok := x.(*Source)
		if !ok {
			return nil, fmt.Errorf(
				"%T cannot be rewritten into %[2]T: %[2]v",
				&n.From, x,
			)
		}
		n2.From = *s
		
		if v, ok := v.(interface {
			VisitWhere(expr.Expr, Visitor) (expr.Expr, error)
		}); ok {
			if n.Where, err = v.VisitWhere(n.Where, v); err != nil {
				return nil, err
			}
		}
		if v, ok := v.(interface {
			VisitSort(*Sort, Visitor) (*Sort, error)
		}); ok {
			for i := range n.OrderBy {
				s, err := v.VisitSort(&n.OrderBy[i], v)
				if err != nil {
					return nil, err
				}
				if s != &n.OrderBy[i] {
					n.OrderBy[i] = *s
				}
			}
		}
	case *Source:
		if v, ok := v.(interface {
			VisitSource(*Source, Visitor) (interface{}, error)
		}); ok {
			return v.VisitSource(n, v)
		}
		if v, ok := v.(JoinVisitor); ok {
			for i := range n.Joins {
				if 
			}
		}
	case *Join:
		if v, ok := v.(JoinVisitor); ok {
			return v.VisitJoin(n, v)
		}
	case *Sort:
		if v, ok := v.(interface {
			VisitSort(*Sort, Visitor) (interface{}, error)
		}); ok {
			return v.VisitSort(n, v)
		}
	}
	if v, ok := v.(interface{
		VisitExpr(expr.Expr, Visitor) (expr.Expr, error)
	}); ok {
		return v.VisitExpr(n, v)
	}
	return n, nil
}
