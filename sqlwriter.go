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

/*
The interfaces in this package have a lot of interdependencies (e.g.
a SQLWriterTo needs an ExprWriterTo in order to generate SQL expressions
but an ExprWriterTo needs a SQLWriterTo for subqueries inside
expressions, etc.) but are written this way in order to have their
components swappable (e.g. for different SQL dialects) and implementable
by external packages.
*/

// ArgWriterTo writes arguments to the SmallWriter and appends the
// arg to currentArgs.  An ArgWriterTo instance is only used to append
// args for a single query or statement (currentArgs is always the same
// slice).
//
// Different implementations might support named arguments which can be
// repeated, so the currentArgs might be returned as-is without
// appending.
//
// if arg implements expr.Var, it should be kept as an expr.Var and
// will be resolved in the args slice at the time the query is executed.
type ArgWriterTo interface {
	WriteArgTo(
		ctx context.Context,
		w SmallWriter,
		currentArgs []interface{},
		arg interface{},
	) (
		appendedArgs []interface{},
		n int64,
		err error,
	)
}

// ExprWriterTo writes SQL expressions.
type ExprWriterTo interface {
	// WriteExprTo writes a SQL expression to w.
	// swt is used to generate subqueries for expressions like
	// `WHERE 123 IN (SELECT ID FROM ...)`.
	// awt is used to write arguments into the SQL expression
	WriteExprTo(
		ctx context.Context,
		w SmallWriter,
		swt SQLWriterTo,
		awt ArgWriterTo,
		currentArgs []interface{},
		e expr.Expr,
	) (
		newArgs []interface{},
		written int64,
		err error,
	)
}

// SQLWriterTo writes SQL statements.
type SQLWriterTo interface {
	// WriteSQLTo writes a SQL statement to w.
	// ewt is used to write the expressions, awt for the arguments.
	WriteSQLTo(
		ctx context.Context,
		w SmallWriter,
		ewt ExprWriterTo,
		awt ArgWriterTo,
		currentArgs []interface{},
		sql sqllang.Node,
	) (
		newArgs []interface{},
		written int64,
		err error,
	)
}

// SQLWriter is used to generate a SQL statement or expression.
type SQLWriter interface {
	// Args gets the current list of arguments.
	Args() []interface{}
	WriteArg(ctx context.Context, arg interface{}) (int64, error)
	WriteExpr(context.Context, expr.Expr) (int64, error)
	WriteSQL(context.Context, sqllang.Node) (int64, error)
}

// sqlWriter is the default SQLWriter implementation that uses
// an ArgWriterTo, ExprWriterTo, and SQLWriterTo to generate SQL.
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

type SQLWriterOption interface {
	applyOptionToSQLWriter(*sqlWriter) error
}

type sqlWriterOptionFunc func(*sqlWriter) error

func (f sqlWriterOptionFunc) applyOptionToSQLWriter(sw *sqlWriter) error {
	return f(sw)
}

var (
	errNoArgWriterTo  = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*ArgWriterTo)(nil)).Elem().Name()))
	errNoExprWriterTo = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*ExprWriterTo)(nil)).Elem().Name()))
	errNoSQLWriterTo  = errors.New(fmt.Sprintf("missing %v", reflect.TypeOf((*SQLWriterTo)(nil)).Elem().Name()))
)

// MakeSQLWriter creates a SQLWriter from the given io.Writer and
// options.
func MakeSQLWriter(ctx context.Context, w io.Writer, options ...SQLWriterOption) (SQLWriter, error) {
	if sw, ok := w.(SQLWriter); ok {
		return sw, nil
	}
	sw := &sqlWriter{}
	sw.init(w)
	for _, opt := range options {
		if err := opt.applyOptionToSQLWriter(sw); err != nil {
			return nil, err
		}
	}
	// TODO: Set defaults?
	if sw.argWriterTo == nil {
		sw.argWriterTo = odbcArgWriterTo{}
	}
	if sw.exprWriterTo == nil {
		sw.exprWriterTo = defaultExprWriterTo{}
	}
	if sw.sqlWriterTo == nil {
		sw.sqlWriterTo = defaultSQLWriterTo{}
	}
	return sw, nil
}

// WithArgWriterTo configures the argument writer for a DB or SQLWriter
func WithArgWriterTo(awt ArgWriterTo) interface {
	DBOption
	SQLWriterOption
} {
	return setArgWriterTo{awt}
}

type setArgWriterTo struct {
	argWriterTo ArgWriterTo
}

func (sawt setArgWriterTo) applyOptionToDB(db *DB) error {
	db.argWriterTo = sawt.argWriterTo
	return nil
}

func (sawt setArgWriterTo) applyOptionToSQLWriter(sw *sqlWriter) error {
	sw.argWriterTo = sawt.argWriterTo
	return nil
}

// WithExprWriterTo configures the argument writer for a DB or SQLWriter
func WithExprWriterTo(ewt ExprWriterTo) interface {
	DBOption
	SQLWriterOption
} {
	return setExprWriterTo{ewt}
}

type setExprWriterTo struct {
	exprWriterTo ExprWriterTo
}

func (sewt setExprWriterTo) applyOptionToDB(db *DB) error {
	db.exprWriterTo = sewt.exprWriterTo
	return nil
}

func (sewt setExprWriterTo) applyOptionToSQLWriter(sw *sqlWriter) error {
	sw.exprWriterTo = sewt.exprWriterTo
	return nil
}

// WithSQLWriterTo configures the argument writer for a DB or SQLWriter
func WithSQLWriterTo(swt SQLWriterTo) interface {
	DBOption
	SQLWriterOption
} {
	return setSQLWriterTo{swt}
}

type setSQLWriterTo struct {
	sqlWriterTo SQLWriterTo
}

func (sswt setSQLWriterTo) applyOptionToDB(db *DB) error {
	db.sqlWriterTo = sswt.sqlWriterTo
	return nil
}

func (sswt setSQLWriterTo) applyOptionToSQLWriter(sw *sqlWriter) error {
	sw.sqlWriterTo = sswt.sqlWriterTo
	return nil
}

func (sw *sqlWriter) Args() []interface{} { return sw.args }

func (sw *sqlWriter) WriteArg(ctx context.Context, arg interface{}) (n int64, err error) {
	sw.args, n, err = sw.argWriterTo.WriteArgTo(ctx, &sw.w, sw.args, arg)
	return
}

func (sw *sqlWriter) WriteExpr(ctx context.Context, e expr.Expr) (n int64, err error) {
	sw.args, n, err = sw.exprWriterTo.WriteExprTo(
		ctx,
		&sw.w,
		sw.sqlWriterTo,
		sw.argWriterTo,
		sw.args,
		e,
	)
	return
}

func (sw *sqlWriter) WriteSQL(ctx context.Context, s sqllang.Node) (n int64, err error) {
	sw.args, n, err = sw.sqlWriterTo.WriteSQLTo(
		ctx,
		&sw.w,
		sw.exprWriterTo,
		sw.argWriterTo,
		sw.args,
		s,
	)
	return
}

type odbcArgWriterTo struct{}

var _ ArgWriterTo = odbcArgWriterTo{}

func (odbcArgWriterTo) WriteArgTo(ctx context.Context, w SmallWriter, currentArgs []interface{}, arg interface{}) (newArgs []interface{}, n int64, err error) {
	if err = w.WriteByte('?'); err != nil {
		return
	}
	n++
	newArgs = append(currentArgs, arg)
	return
}

type defaultExprWriterTo struct{}

func (defaultExprWriterTo) WriteExprTo(
	ctx context.Context,
	w SmallWriter,
	swt SQLWriterTo,
	awt ArgWriterTo,
	currentArgs []interface{},
	e expr.Expr,
) (
	newArgs []interface{},
	n int64,
	err error,
) {
	swc := smallWriterCounterOf(w)
	start := swc.written
	v := &defaultExprWriterVisitor{
		w:           swc,
		argWriterTo: awt,
		sqlWriterTo: swt,
		args:        currentArgs,
		stack:       make([]defaultExprWriterVisitorFrame, 1, arbitraryCapacity),
	}
	err = expr.Walk(ctx, e, v)
	n = swc.written - start
	newArgs = v.args
	return
}

type defaultExprWriterVisitor struct {
	w           *smallWriterCounter
	argWriterTo ArgWriterTo
	sqlWriterTo SQLWriterTo
	args        []interface{}
	stack       []defaultExprWriterVisitorFrame
	queries     []query
}

var _ expr.Visitor = (*defaultExprWriterVisitor)(nil)

func (vis *defaultExprWriterVisitor) Visit(ctx context.Context, e expr.Expr) (v2 expr.Visitor, err error) {
	if e != nil {
		vis.stack = append(vis.stack, defaultExprWriterVisitorFrame{})
		top := &vis.stack[len(vis.stack)-1]
		top.e = e
		sec := &vis.stack[len(vis.stack)-2]
		if sec.e != nil && precedenceOf(sec.e) > precedenceOf(top.e) {
			if err = vis.w.WriteByte('('); err != nil {
				return nil, err
			}
		}
		switch e.(type) {
		case expr.Not:
			if _, err = vis.w.WriteString("NOT "); err != nil {
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
		return vis, nil
	}
	top := &vis.stack[len(vis.stack)-1]
	e = top.e
	if _, err = vis.w.WriteString(top.infix); err != nil {
		return nil, err
	}
	top.infix = ""
	switch e := e.(type) {
	case query:
		name := e.Name()
		if _, err := vis.w.WriteString(name); err != nil {
			return nil, fmt.Errorf(
				"writing query %#v name %q: %w",
				e, name, err,
			)
		}
	case expr.Var:
		vis.args, _, err = vis.argWriterTo.WriteArgTo(ctx, vis.w, vis.args, e)
		if err != nil {
			return nil, fmt.Errorf(
				"visiting var %#v: %w",
				e, err,
			)
		}
	default:
		vis.args, _, err = vis.argWriterTo.WriteArgTo(ctx, vis.w, vis.args, e)
		if err != nil {
			return nil, fmt.Errorf(
				"visiting arg %#v: %w",
				e, err,
			)
		}
	}
	sec := &vis.stack[len(vis.stack)-2]
	if sec.e != nil && precedenceOf(sec.e) > precedenceOf(top.e) {
		if err = vis.w.WriteByte(')'); err != nil {
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

type defaultSQLWriterTo struct{}

var _ SQLWriterTo = defaultSQLWriterTo{}

func (defaultSQLWriterTo) WriteSQLTo(
	ctx context.Context,
	w SmallWriter,
	ewt ExprWriterTo,
	awt ArgWriterTo,
	currentArgs []interface{},
	node sqllang.Node,
) (
	newArgs []interface{},
	written int64,
	err error,
) {
	swc := smallWriterCounterOf(w)
	start := swc.written
	v := &defaultSQLWriterVisitor{
		w:            swc,
		argWriterTo:  awt,
		exprWriterTo: ewt,
		sqlWriterTo:  defaultSQLWriterTo{},
		args:         currentArgs,
	}
	err = sqllang.Walk(ctx, node, v)
	written = swc.written - start
	newArgs = v.args
	return
}

type defaultSQLWriterVisitor struct {
	w            *smallWriterCounter
	argWriterTo  ArgWriterTo
	exprWriterTo ExprWriterTo
	sqlWriterTo  SQLWriterTo
	args         []interface{}
	stack        []defaultSQLWriterVisitorFrame
}

func (vis *defaultSQLWriterVisitor) Visit(ctx context.Context, node sqllang.Node) (sqllang.Visitor, error) {
	entering := node != nil
	if entering {
		vis.stack = append(vis.stack, defaultSQLWriterVisitorFrame{
			node: node,
		})
	} else {
		node = vis.stack[len(vis.stack)-1].node
		vis.stack = vis.stack[:len(vis.stack)-1]
	}
	switch node := node.(type) {
	case *sqllang.Select:
		if entering && len(vis.stack) > 1 {
			if err := vis.w.WriteByte('('); err != nil {
				return nil, fmt.Errorf("writing '(': %w", err)
			}
		}
		if _, err := vis.w.WriteString("SELECT "); err != nil {
			return nil, fmt.Errorf("writing 'SELECT': %w", err)
		}
		if !entering && len(vis.stack) > 0 {
			if err := vis.w.WriteByte(')'); err != nil {
				return nil, fmt.Errorf("writing ')': %w", err)
			}
		}
	case *sqllang.Column:
		if entering {
			if vis.top(1).wroteColumn {
				if _, err := vis.w.WriteString(", "); err != nil {
					return nil, err
				}
			}
		} else {
			vis.top(1).wroteColumn = true
		}
	case *sqllang.Source:
		if entering {
			if _, err := vis.w.WriteString(" FROM "); err != nil {
				return nil, err
			}
			if node.Table != "" {
				if _, err := vis.w.WriteString(node.Table); err != nil {
					return nil, err
				}
				if node.Alias != "" {
					if err := vis.w.WriteByte(' '); err != nil {
						return nil, err
					}
					if _, err := vis.w.WriteString(node.Alias); err != nil {
						return nil, err
					}
				}
			}
		} else if node.Select != nil && node.Alias != "" {
			if err := vis.w.WriteByte(' '); err != nil {
				return nil, err
			}
			if _, err := vis.w.WriteString(node.Alias); err != nil {
				return nil, err
			}
		}
	case *sqllang.Join:
		if entering {
			if _, err := vis.w.WriteString(" INNER JOIN "); err != nil {
				return nil, err
			}
		} else {
			if _, err := vis.w.WriteString(" ON "); err != nil {
				return nil, err
			}
			if err := vis.writeExpr(ctx, "join on", node.On); err != nil {
				return nil, err
			}
		}
	case *sqllang.Where:
		if entering {
			if _, err := vis.w.WriteString(" WHERE "); err != nil {
				return nil, err
			}
			if err := vis.writeExpr(ctx, "where", node.Expr); err != nil {
				return nil, err
			}
		}
	case *sqllang.Sort:
		if entering {
			var str string
			if !vis.top(1).wroteColumn {
				str = " ORDER BY "
			} else {
				str = ", "
			}
			if _, err := vis.w.WriteString(str); err != nil {
				return nil, err
			}
			if err := vis.writeExpr(ctx, "sort", node.By); err != nil {
				return nil, err
			}
			if node.Desc {
				str = " DESC"
			} else {
				str = " ASC"
			}
			if _, err := vis.w.WriteString(str); err != nil {
				return nil, err
			}
		}
	}
	return vis, nil
}

func (vis *defaultSQLWriterVisitor) writeExpr(ctx context.Context, what string, e expr.Expr) (err error) {
	if vis.args, _, err = vis.exprWriterTo.WriteExprTo(
		ctx,
		vis.w,
		vis.sqlWriterTo,
		vis.argWriterTo,
		vis.args,
		e,
	); err != nil {
		err = fmt.Errorf(
			"writing %v %#v: %w",
			what, e, err,
		)
	}
	return
}

type defaultSQLWriterVisitorFrame struct {
	node        sqllang.Node
	wroteColumn bool
}

func (vis *defaultSQLWriterVisitor) top(i int) *defaultSQLWriterVisitorFrame {
	return &vis.stack[len(vis.stack)-i-1]
}
