package sqlstream

import (
	"context"
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
	w      smallWriterCounter
	dbInfo *DBInfo
	args   []interface{}
}

var _ interface {
	SQLWriter
} = (*sqlWriter)(nil)

func (sw *sqlWriter) init(dbi *DBInfo, w io.Writer) {
	sw.dbInfo = dbi
	sw.w.smallWriter = SmallWriterOf(w)
}

//type sqlWriterOptionFunc func(*sqlWriter) error

// func (f sqlWriterOptionFunc) applyOptionToSQLWriter(sw *sqlWriter) error {
// 	return f(sw)
// }

// WithArgWriterTo configures the argument writer for a DB or SQLWriter
func WithArgWriterTo(awt ArgWriterTo) interface {
	DBOption
	DBInfoOption
} {
	return dbDBInfoOptionFunc(func(dbi *DBInfo) error {
		dbi.ArgWriterTo = awt
		return nil
	})
}

// WithExprWriterTo configures the argument writer for a DB or SQLWriter
func WithExprWriterTo(ewt ExprWriterTo) interface {
	DBOption
	DBInfoOption
} {
	return dbDBInfoOptionFunc(func(dbi *DBInfo) error {
		dbi.ExprWriterTo = ewt
		return nil
	})
}

// WithSQLWriterTo configures the argument writer for a DB or SQLWriter
func WithSQLWriterTo(swt SQLWriterTo) interface {
	DBOption
	DBInfoOption
} {
	return dbDBInfoOptionFunc(func(dbi *DBInfo) error {
		dbi.SQLWriterTo = swt
		return nil
	})
}

func (sw *sqlWriter) Args() []interface{} { return sw.args }

func (sw *sqlWriter) WriteArg(ctx context.Context, arg interface{}) (n int64, err error) {
	sw.args, n, err = sw.dbInfo.ArgWriterTo.WriteArgTo(ctx, &sw.w, sw.args, arg)
	return
}

func (sw *sqlWriter) WriteExpr(ctx context.Context, e expr.Expr) (n int64, err error) {
	sw.args, n, err = sw.dbInfo.ExprWriterTo.WriteExprTo(
		ctx,
		&sw.w,
		sw.dbInfo.SQLWriterTo,
		sw.dbInfo.ArgWriterTo,
		sw.args,
		e,
	)
	return
}

func (sw *sqlWriter) WriteSQL(ctx context.Context, s sqllang.Node) (n int64, err error) {
	sw.args, n, err = sw.dbInfo.SQLWriterTo.WriteSQLTo(
		ctx,
		&sw.w,
		sw.dbInfo.ExprWriterTo,
		sw.dbInfo.ArgWriterTo,
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
}

var _ expr.Visitor = (*defaultExprWriterVisitor)(nil)

func (vis *defaultExprWriterVisitor) Visit(ctx context.Context, e expr.Expr) (v2 expr.Visitor, err error) {
	if e != nil {
		vis.stack = append(vis.stack, defaultExprWriterVisitorFrame{})
		top := &vis.stack[len(vis.stack)-1]
		top.e = e
		sec := &vis.stack[len(vis.stack)-2]
		if sec.e != nil && precedenceOf(sec.e) < precedenceOf(top.e) {
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
	sec := &vis.stack[len(vis.stack)-2]
	defaultCase := func(e expr.Expr) error {
		vis.args, _, err = vis.argWriterTo.WriteArgTo(ctx, vis.w, vis.args, e)
		if err != nil {
			return fmt.Errorf(
				"visiting arg %#v: %w",
				e, err,
			)
		}
		return nil
	}
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
	case *reflect.StructField:
		if mem, ok := sec.e.(expr.Mem); ok {
			q, ok := mem[0].(query)
			if !ok {
				return nil, fmt.Errorf(
					"cannot get member of non-query: %v (type: %[1]T)",
					mem[0],
				)
			}
			tq, _ := tableQueryOf(q)
			mt := tq.table.modelType
			fieldIndex := func() int {
				for i := range mt.structFields {
					for _, fp := range mt.structFields[i].urFieldPath {
						if &fp.urType.ReflectStructFields()[fp.fieldIndex] == e {
							return i
						}
					}
				}
				return -1
			}()
			if fieldIndex == -1 {
				return nil, fmt.Errorf(
					"failed to find member %v of %v",
					e.Name,
					tq.table.modelType.unsafereflectType.ReflectType().Name(),
				)
			}
			colName := tq.table.sqlTable.Columns[fieldIndex].ColumnName.Name
			if err := vis.w.WriteByte('"'); err != nil {
				return nil, fmt.Errorf(
					"writing quote before column %v: %w",
					colName, err,
				)
			}
			if _, err := vis.w.WriteString(colName); err != nil {
				return nil, fmt.Errorf(
					"writing column %v name: %w",
					colName, err,
				)
			}
			if err := vis.w.WriteByte('"'); err != nil {
				return nil, fmt.Errorf(
					"writing quote after column %v: %w",
					colName, err,
				)
			}
		} else if err := defaultCase(e); err != nil {
			return nil, err
		}
	case expr.Unary:
	case expr.Binary:
	case interface{ Operands() []expr.Expr }:
		// pass
	default:
		if err := defaultCase(e); err != nil {
			return nil, err
		}
	}
	if _, err = vis.w.WriteString(sec.infix); err != nil {
		return nil, err
	}
	sec.infix = ""
	if sec.e != nil && precedenceOf(sec.e) < precedenceOf(top.e) {
		if err = vis.w.WriteByte(')'); err != nil {
			return nil, err
		}
	}
	vis.stack = vis.stack[:len(vis.stack)-1]
	return
}

var precedences = func() (precedences [][]reflect.Type) {
	precedencePtrsTo := [][]interface{}{
		// It seems SQL doesn't treat . as an operator, so I'll
		// just put it at the highest precedence
		//{(*expr.Mem)(nil)},
		// TODO: expr.Neg
		{(*expr.Mul)(nil), (*expr.Div)(nil) /* TODO: expr.Mod */},
		{(*expr.Add)(nil), (*expr.Sub)(nil) /* TODO: Bitwise */},
		{
			// Comparison operators
			(*expr.Eq)(nil),
			(*expr.Ne)(nil),
			(*expr.Gt)(nil),
			(*expr.Ge)(nil),
			(*expr.Lt)(nil),
			(*expr.Le)(nil),
		},
		{(*expr.Not)(nil)},
		{(*expr.And)(nil)},
		{(*expr.Or)(nil)},
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

func (swt defaultSQLWriterTo) WriteSQLTo(
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
		sqlWriterTo:  swt,
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
		if entering {
			if len(vis.stack) > 1 {
				if err := vis.w.WriteByte('('); err != nil {
					return nil, fmt.Errorf("writing '(': %w", err)
				}
			}
			if _, err := vis.w.WriteString("SELECT "); err != nil {
				return nil, fmt.Errorf("writing 'SELECT': %w", err)
			}
		} else if len(vis.stack) > 0 {
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
			vis.top(0).wroteColumn = true
			if err := vis.writeExpr(ctx, "column", node.Expr); err != nil {
				return nil, fmt.Errorf("writing column: %w", err)
			}
			return vis, vis.writeAlias(ctx, node.Alias)
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

func (vis *defaultSQLWriterVisitor) writeAlias(ctx context.Context, alias string) (err error) {
	if alias == "" {
		return nil
	}
	if err = vis.w.WriteByte(' '); err != nil {
		return err
	}
	_, err = vis.w.WriteString(alias)
	return
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
