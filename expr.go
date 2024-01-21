package sqlstream

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

// ExprWriterTo is used to write expression trees into SQL expression strings
type ExprWriterTo interface {
	Args() []interface{}
	WriteExprTo(ctx context.Context, w io.Writer, e expr.Expr) (int64, error)
}

func tableAliasOf(ctx context.Context, ew ExprWriterTo, q query) (alias string, err error) {
	sb := strings.Builder{}
	if _, err = ew.WriteExprTo(ctx, &sb, q); err != nil {
		return
	}
	return sb.String(), nil
}

// exprWriterToV1 is a default ExprWriterTo implementation.
type exprWriterToV1 struct {
	args     []interface{}
	prefixes map[anyTable]*ewV1Prefix
	aliases  map[query]string
}

var _ interface {
	ExprWriterTo
} = (*exprWriterToV1)(nil)

func (ewt *exprWriterToV1) Args() []interface{} { return ewt.args }

func (ewt *exprWriterToV1) WriteArgTo(ctx context.Context, w io.Writer, v interface{}) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if va, ok := v.(expr.Var); ok {
		vs, err := expr.ValuesFromContext(ctx)
		if err != nil {
			return 0, err
		}
		v, err = vs.Get(ctx, va)
		if err != nil {
			return 0, err
		}
	}
	n, err := w.Write(constBytes[qmarkIndex : qmarkIndex+1])
	if err == nil {
		ewt.args = append(ewt.args, v)
	}
	return int64(n), err
}

func (ewt *exprWriterToV1) WriteExprTo(ctx context.Context, w io.Writer, e expr.Expr) (int64, error) {
	ew := exprWriterV1{
		smallWriterCounter: smallWriterCounterOf(w),
		exprWriterToV1:     ewt,
		stack:              make([]ewV1Frame, 1, arbitraryCapacity),
	}
	err := expr.Walk(ctx, e, &ew)
	return ew.smallWriterCounter.written, err
}

func (ewt *exprWriterToV1) aliasOf(q query) string {
	al, ok := ewt.aliases[q]
	if ok {
		return al
	}
	prefixOf := func(ewt *exprWriterToV1, t anyTable) *ewV1Prefix {
		ep, ok := ewt.prefixes[t]
		if ok {
			return ep
		}
		ep = &ewV1Prefix{}
		alias := make([]rune, 0, 8)
		space := false
		for i, r := range t.anyModelType().Name() {
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
	makeAlias := func(ewt *exprWriterToV1, q query) string {
		t, _ := tableOfQuery(q)
		oswp := prefixOf(ewt, t)
		alias := oswp.prefix + strconv.FormatInt(int64(oswp.count), 10)
		oswp.count++
		return alias
	}
	al = makeAlias(ewt, q)
	ewt.aliases[q] = al
	return al
}

type ewV1Prefix struct {
	prefix string
	count  int
}

type ewV1Frame struct {
	e     expr.Expr
	infix string
}

type exprWriterV1 struct {
	*smallWriterCounter
	*exprWriterToV1
	stack []ewV1Frame
}

func (ew *exprWriterV1) Visit(ctx context.Context, e expr.Expr) (expr.Visitor, error) {
	if e != nil {
		switch e.(type) {
		case expr.Mem:
		case expr.Binary:
			if err := ew.smallWriterCounter.WriteByte('('); err != nil {
				return nil, err
			}
		}
		ew.stack = append(ew.stack, ewV1Frame{
			e: e,
		})
		top := &ew.stack[len(ew.stack)-1]
		switch e.(type) {
		case expr.Not:
			if _, err := ew.smallWriterCounter.WriteString("NOT "); err != nil {
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
		if err := ew.smallWriterCounter.WriteByte(')'); err != nil {
			return nil, err
		}
	}
	writeIdent := func(ctx context.Context, ew *exprWriterV1, id sqllang.Ident) error {
		if err := ew.WriteByte('"'); err != nil {
			return err
		}
		if _, err := ew.WriteString(id.String()); err != nil {
			return err
		}
		return ew.WriteByte('"')
	}
	defaultWrite := func(ctx context.Context, ew *exprWriterV1, e expr.Expr) error {
		_, err := ew.WriteArgTo(ctx, ew.smallWriterCounter, e)
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
				if _, err := ew.smallWriterCounter.WriteString("1 = "); err != nil {
					return nil, err
				}
			}
			var b byte
			if e {
				b = '1' // TODO: I think BITs in Oracle are 2's complement (== -1)
			} else {
				b = '0'
			}
			if err := ew.smallWriterCounter.WriteByte(b); err != nil {
				return nil, err
			}
		case query:
			if _, err := ew.smallWriterCounter.WriteString(ew.aliasOf(e)); err != nil {
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
				tq := tableQueryV1Of(q)
				name := tq.table.Columns()[e.Index[0]].Name
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
		if _, err := ew.smallWriterCounter.WriteString(sec.infix); err != nil {
			return nil, err
		}
		sec.infix = ""
	}
	return ew, nil
}

func (ew *exprWriterV1) pop() (top ewV1Frame) {
	ptop := &ew.stack[len(ew.stack)-1]
	top, *ptop = *ptop, ewV1Frame{}
	ew.stack = ew.stack[:len(ew.stack)-1]
	return
}
