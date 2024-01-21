package sqllang

import "context"

type Node interface {
	sqllangNode()
}

type Visitor interface {
	Visit(context.Context, Node) (Visitor, error)
}

func Walk(ctx context.Context, n Node, v Visitor) (err error) {
	v, err = v.Visit(ctx, n)
	if err != nil || v == nil {
		return
	}
	switch n := n.(type) {
	case *Select:
		for i := range n.Columns {
			if err = Walk(ctx, &n.Columns[i], v); err != nil {
				return
			}
		}
		if err = Walk(ctx, &n.From, v); err != nil {
			return
		}
		if err = Walk(ctx, &n.Where, v); err != nil {
			return
		}
		for i := range n.OrderBy {
			if err = Walk(ctx, &n.OrderBy[i], v); err != nil {
				return
			}
		}
	case *Source:
		for i := range n.Joins {
			if err = Walk(ctx, &n.Joins[i], v); err != nil {
				return
			}
		}
		if n.Select != nil {
			if err = Walk(ctx, n.Select, v); err != nil {
				return
			}
		}
	case *Join:
		return Walk(ctx, &n.Source, v)
	}
	_, err = v.Visit(ctx, nil)
	return
}
