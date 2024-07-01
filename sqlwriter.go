package sqlstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

type ArgWriterTo interface {
	WriteArgTo(ctx context.Context, w SmallWriter, arg interface{}, currentArgs []interface{}) (newArgs []interface{}, n int64, err error)
}

type ExprWriterTo interface {
	WriteExprTo(ctx context.Context, w SmallWriter, e expr.Expr) (int64, error)
}

type SQLWriterTo interface {
	WriteSQLTo(ctx context.Context, w SmallWriter, sql sqllang.Node) (int64, error)
}

type SQLWriter interface {
	Args() []interface{}
	WriteArg(ctx context.Context, arg interface{}) (int64, error)
	WriteExpr(context.Context, expr.Expr) (int64, error)
	WriteSQL(context.Context, sqllang.Node) (int64, error)
}

type sqlWriter struct {
	w            smallWriterCounter
	argWriterTo  ArgWriterTo
	exprWriterTo ExprWriterTo
	sqlWriterTo  SQLWriterTo
	args         []interface{}
}

var _ interface {
	SQLWriter
} = (*sqlWriter)(nil)

func (sw *sqlWriter) init(w io.Writer) {
	sw.w.smallWriter = SmallWriterOf(w)
}

type SQLWriterOption func(*sqlWriter) error

var (
	errNoArgWriterTo  = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*ArgWriterTo)(nil)).Elem().Name()))
	errNoExprWriterTo = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*ExprWriterTo)(nil)).Elem().Name()))
	errNoSQLWriterTo  = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*SQLWriterTo)(nil)).Elem().Name()))
)

func SQLWriterOf(ctx context.Context, w io.Writer, options ...SQLWriterOption) (SQLWriter, error) {
	if sw, ok := w.(SQLWriter); ok {
		return sw, nil
	}
	sw := &sqlWriter{}
	sw.init(w)
	for _, opt := range options {
		if err := opt(sw); err != nil {
			return nil, err
		}
	}
	// TODO: Set defaults?
	if sw.argWriterTo == nil {
		return nil, errNoArgWriterTo
	}
	if sw.exprWriterTo == nil {
		return nil, errNoExprWriterTo
	}
	if sw.sqlWriterTo == nil {
		return nil, errNoSQLWriterTo
	}
	return sw, nil
}

func (sw *sqlWriter) Args() []interface{} { return sw.args }

func (sw *sqlWriter) WriteArg(ctx context.Context, arg interface{}) (n int64, err error) {
	start := sw.w.written
	sw.args, n, err = sw.argWriterTo.WriteArgTo(ctx, &sw.w, arg, sw.args)
	n -= start
	return
}

func (sw *sqlWriter) WriteExpr(ctx context.Context, e expr.Expr) (n int64, err error) {
	start := sw.w.written
	n, err = sw.exprWriterTo.WriteExprTo(ctx, &sw.w, e)
	n -= start
	return
}

func (sw *sqlWriter) WriteSQL(ctx context.Context, s sqllang.Node) (n int64, err error) {
	start := sw.w.written
	n, err = sw.sqlWriterTo.WriteSQLTo(ctx, &sw.w, s)
	n -= start
	return
}

type odbcArgWriterTo struct{}

var _ ArgWriterTo = odbcArgWriterTo{}

func (odbcArgWriterTo) WriteArgTo(ctx context.Context, w SmallWriter, arg interface{}, currentArgs []interface{}) (newArgs []interface{}, n int64, err error) {
	if err = w.WriteByte('?'); err != nil {
		return
	}
	n++
	newArgs = append(newArgs, arg)
	return
}

type defaultExprWriterTo struct{}

func (defaultExprWriterTo) WriteExprTo(ctx context.Context, w SmallWriter, e expr.Expr) (n int64, err error) {
	swc := smallWriterCounterOf(w)
	start := swc.written
	v := &defaultExprWriterVisitor{
		w:     swc,
		stack: make([]defaultExprWriterVisitorFrame, 1, arbitraryCapacity),
	}
	err = expr.Walk(ctx, e, v)
	n = swc.written - start
	return
}

type defaultExprWriterVisitor struct {
	w       *smallWriterCounter
	stack   []defaultExprWriterVisitorFrame
	queries []query
}

var _ expr.Visitor = (*defaultExprWriterVisitor)(nil)

func (v *defaultExprWriterVisitor) Visit(ctx context.Context, e expr.Expr) (v2 expr.Visitor, err error) {
	if e != nil {
		v.stack = append(v.stack, defaultExprWriterVisitorFrame{})
		top := &v.stack[len(v.stack)-1]
		top.e = e
		sec := &v.stack[len(v.stack)-2]
		if sec.e != nil && precedenceOf(sec.e) > precedenceOf(top.e) {
			if err = v.w.WriteByte('('); err != nil {
				return nil, err
			}
		}
		switch e.(type) {
		case expr.Not:
			if _, err = v.w.WriteString("NOT "); err != nil {
				return nil, err
			}
		case expr.Eq:
			top.infix = " = "
		case expr.Ne:
			top.infix = " <> "
		case expr.Gt:
			top.infix = " > "
		case expr.Ge:
			top.infix = " >= "
		case expr.Lt:
			top.infix = " < "
		case expr.Le:
			top.infix = " <= "
		case expr.And:
			top.infix = " AND "
		case expr.Or:
			top.infix = " OR "
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
		}
		return v, nil
	}
	top := &v.stack[len(v.stack)-1]
	e = top.e
	if _, err = v.w.WriteString(top.infix); err != nil {
		return nil, err
	}
	top.infix = ""
	switch e := e.(type) {
	case expr.NamedVar:
		if _, err = v.w.WriteString(e.Name()); err != nil {
			return nil, err
		}
	default:

	}
	sec := &v.stack[len(v.stack)-2]
	if sec.e != nil && precedenceOf(sec.e) > precedenceOf(top.e) {
		if err = v.w.WriteByte(')'); err != nil {
			return nil, err
		}
	}
	return
}

var precedences = func() (precedences [][]reflect.Type) {
	precedencePtrsTo := [][]interface{}{
		// It seems SQL doesn't treat . as an operator, so I'll
		// just put it at the highest precedence
		[]interface{}{(*expr.Mem)(nil)},
		// TODO: expr.Neg
		[]interface{}{(*expr.Mul)(nil), (*expr.Div)(nil) /* TODO: expr.Mod */},
		[]interface{}{(*expr.Add)(nil), (*expr.Sub)(nil) /* TODO: Bitwise */},
		[]interface{}{
			// Comparison operators
			(*expr.Eq)(nil),
			(*expr.Ne)(nil),
			(*expr.Gt)(nil),
			(*expr.Ge)(nil),
			(*expr.Lt)(nil),
			(*expr.Le)(nil),
		},
		[]interface{}{(*expr.Not)(nil)},
		[]interface{}{(*expr.And)(nil)},
		[]interface{}{(*expr.Or)(nil)},
		/* ALL, ANY, BETWEEN, IN, LIKE, OR, SOME */
	}
	precedences = make([][]reflect.Type, len(precedencePtrsTo))
	precCache := func() []reflect.Type {
		length := 0
		for _, ptrs := range precedencePtrsTo {
			length += len(ptrs)
		}
		return make([]reflect.Type, length)
	}()
	for i, ptrs := range precedencePtrsTo {
		precedences[i] = precCache[:len(ptrs)]
		precCache = precCache[len(ptrs):]
		for j, p := range ptrs {
			precedences[i][j] = reflect.TypeOf(p).Elem()
		}
	}
	return
}()

func precedenceOf(e expr.Expr) int {
	t := reflect.TypeOf(e)
	for i, precs := range precedences {
		for _, p := range precs {
			if p == t {
				return i
			}
		}
	}
	return -1 // assume it's not an operator?
}

type defaultExprWriterVisitorFrame struct {
	e     expr.Expr
	infix string
}
