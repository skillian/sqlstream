package sqlstream

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"unicode"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

type ExprWriterer interface {
	ExprWriter(ctx context.Context, w io.Writer) (ExprWriter, error)
}

type ExprWritererFunc func(context.Context, io.Writer) (ExprWriter, error)

func (f ExprWritererFunc) ExprWriter(ctx context.Context, w io.Writer) (ExprWriter, error) {
	return f(ctx, w)
}

// ExprWriter is used to write expression trees into SQL expression strings
type ExprWriter interface {
	Args() []interface{}
	WriteExpr(ctx context.Context, e expr.Expr) (int64, error)
}

type ExprWriterFunc func(context.Context, expr.Expr) (int64, error)

func (f ExprWriterFunc) WriteExpr(ctx context.Context, e expr.Expr) (int64, error) {
	return f(ctx, e)
}

type exprWriter struct {
	stack    []ewFrame
	w        smallWriterCounter
	prefixes map[*table]*ewPrefix
	aliases  map[query]string
	params   Parameters
	dialect  Dialect
}

type ewFrame struct {
	e     expr.Expr
	infix string
}

type ewPrefix struct {
	prefix string
	count  int
}

var _ interface {
	ExprWriter
} = (*exprWriter)(nil)

func (ew *exprWriter) init(w io.Writer, ps Parameters, d Dialect) {
	ew.w.smallWriter = smallWriterOf(w)
	ew.prefixes = make(map[*table]*ewPrefix)
	ew.aliases = make(map[query]string)
	ew.params = ps
	ew.dialect = d
}

func (ew *exprWriter) Args() []interface{} { return ew.params.Args() }

func (ew *exprWriter) WriteExpr(ctx context.Context, e expr.Expr) (int64, error) {
	start := ew.w.written
	err := expr.Walk(ctx, e, (*exprWriterVisitor)(ew))
	return ew.w.written - start, err
}

type exprWriterVisitor exprWriter

func (ewt *exprWriterVisitor) aliasOf(q query) string {
	al, ok := ewt.aliases[q]
	if ok {
		return al
	}
	prefixOf := func(ewt *exprWriterVisitor, t *table) *ewPrefix {
		ep, ok := ewt.prefixes[t]
		if ok {
			return ep
		}
		ep = &ewPrefix{}
		alias := make([]rune, 0, 8)
		space := false
		for i, r := range t.modelType.name {
			switch {
			case i == 0:
				alias = append(alias, unicode.ToLower(r))
			case unicode.IsSpace(r):
				space = true
			case space:
				alias = append(alias, unicode.ToLower(r))
				space = false
			}
		}
		ep.prefix = string(alias)
		ewt.prefixes[t] = ep
		return ep
	}
	makeAlias := func(ewt *exprWriterVisitor, q query) string {
		tq := tableQueryOf(q)
		oswp := prefixOf(ewt, tq.table)
		alias := oswp.prefix + strconv.FormatInt(int64(oswp.count), 10)
		oswp.count++
		return alias
	}
	al = makeAlias(ewt, q)
	ewt.aliases[q] = al
	return al
}

func (ew *exprWriterVisitor) Visit(ctx context.Context, e expr.Expr) (expr.Visitor, error) {
	if e != nil {
		switch e.(type) {
		case expr.Mem:
		case expr.Binary:
			if err := ew.w.WriteByte('('); err != nil {
				return nil, err
			}
		}
		ew.stack = append(ew.stack, ewFrame{
			e: e,
		})
		top := &ew.stack[len(ew.stack)-1]
		switch e.(type) {
		case expr.Not:
			if _, err := ew.w.WriteString("NOT "); err != nil {
				return nil, err
			}
		case expr.Eq:
			top.infix = " = "
		case expr.Ne:
			top.infix = " <> "
		case expr.Lt:
			top.infix = " < "
		case expr.Le:
			top.infix = " <= "
		case expr.Gt:
			top.infix = " > "
		case expr.Ge:
			top.infix = " >= "
		case expr.Add:
			top.infix = " + "
		case expr.Sub:
			top.infix = " - "
		case expr.Mul:
			top.infix = " * "
		case expr.Div:
			top.infix = " / "
		case expr.Mem:
			top.infix = "."
		case expr.And:
			top.infix = " AND "
		case expr.Or:
			top.infix = " OR "
		}
		return ew, nil
	}
	top := ew.pop()
	sec := &ew.stack[len(ew.stack)-1]
	e = top.e
	switch e.(type) {
	case expr.Mem:
	case expr.Binary:
		if err := ew.w.WriteByte(')'); err != nil {
			return nil, err
		}
	}
	writeIdent := func(ctx context.Context, ew *exprWriterVisitor, id sqllang.Ident) (err error) {
		_, err = ew.dialect.Quote(&ew.w, id.String())
		return
	}
	defaultWrite := func(ctx context.Context, ew *exprWriterVisitor, e expr.Expr) error {
		_, err := ew.params.WriteParameterTo(&ew.w, e)
		return err
	}
	if !expr.HasOperands(e) {
		switch e := e.(type) {
		case bool:
			if sec.e == nil || (func() bool {
				switch sec.e.(type) {
				case expr.And, expr.Or:
					return true
				}
				return false
			})() {
				if _, err := ew.w.WriteString("1 = "); err != nil {
					return nil, err
				}
			}
			var b byte
			if e {
				b = '1' // TODO: I think BITs in Oracle are 2's complement (== -1)
			} else {
				b = '0'
			}
			if err := ew.w.WriteByte(b); err != nil {
				return nil, err
			}
		case query:
			if _, err := ew.w.WriteString(ew.aliasOf(e)); err != nil {
				return nil, err
			}
		case *reflect.StructField:
			if m, ok := sec.e.(expr.Mem); ok {
				// TODO: Write the column name
				q, ok := m[0].(query)
				if !ok {
					return nil, errors.Errorf(
						"Expected %v, not %[2]v (type: %[2]T)",
						reflect.TypeOf((*query)(nil)).Elem().Name(),
						m[0],
					)
				}
				tq := tableQueryOf(q)
				name := tq.table.columns[e.Index[0]].name
				if err := writeIdent(ctx, ew, sqllang.IdentFromString(name)); err != nil {
					return nil, err
				}
			} else {
				if err := defaultWrite(ctx, ew, e); err != nil {
					return nil, err
				}
			}
		case sqllang.Ident:
			if err := writeIdent(ctx, ew, e); err != nil {
				return nil, err
			}
		default:
			if err := defaultWrite(ctx, ew, e); err != nil {
				return nil, err
			}
		}
	}
	if len(sec.infix) != 0 {
		if _, err := ew.w.WriteString(sec.infix); err != nil {
			return nil, err
		}
		sec.infix = ""
	}
	return ew, nil
}

func (ew *exprWriterVisitor) pop() (top ewFrame) {
	ptop := &ew.stack[len(ew.stack)-1]
	top, *ptop = *ptop, ewFrame{}
	ew.stack = ew.stack[:len(ew.stack)-1]
	return
}
