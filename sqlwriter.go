package sqlstream

import (
	"context"
	"io"

	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

// SQLWriterer creates a SQLWriter
type SQLWriterer interface {
	// SQLWriter creates a SQLWriter into which SQL expressions
	// and statements can be written.
	SQLWriter(ctx context.Context, w io.Writer) (SQLWriter, error)
}

// SQLWriter writes values as SQL into an `io.Writer`.
type SQLWriter interface {
	ExprWriter

	// WriteSQL writes a SQL node to a writer.
	WriteSQL(ctx context.Context, sql sqllang.Node) (int64, error)
}

type commonSQLWriter struct {
	stack []cswFrame
	exprWriter
}

type cswFrame struct {
	node    sqllang.Node
	suffix  string
	columns int
}

var _ interface {
	SQLWriter
} = (*commonSQLWriter)(nil)

func (csw *commonSQLWriter) init(w io.Writer, ps Parameters, d Dialect) {
	csw.exprWriter.init(w, ps, d)
}

func (csw *commonSQLWriter) WriteSQL(ctx context.Context, sql sqllang.Node) (int64, error) {
	start := csw.exprWriter.w.written
	err := sqllang.Walk(ctx, sql, (*commonSQLWriterVisitor)(csw))
	return csw.exprWriter.w.written - start, err
}

type commonSQLWriterVisitor commonSQLWriter

var _ interface {
	sqllang.Visitor
} = (*commonSQLWriterVisitor)(nil)

func (sw *commonSQLWriterVisitor) writeAlias(alias string) error {
	if alias == "" {
		return nil
	}
	if _, err := sw.w.Write(constBytes[spaceIndex : spaceIndex+1]); err != nil {
		return err
	}
	_, err := sw.w.WriteString(alias)
	return err
}

func (sw *commonSQLWriterVisitor) writeExpr(ctx context.Context, e expr.Expr) error {
	_, err := sw.WriteExpr(ctx, e)
	return err
}

func (sw *commonSQLWriterVisitor) top(skip int) *cswFrame {
	return &sw.stack[len(sw.stack)-1-skip]
}

func (sw *commonSQLWriterVisitor) Visit(ctx context.Context, sn sqllang.Node) (sqllang.Visitor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sn != nil {
		return sw.visitEnter(ctx, sn)
	}
	return sw.visitExit(ctx)
}

func (sw *commonSQLWriterVisitor) visitEnter(ctx context.Context, sn sqllang.Node) (sqllang.Visitor, error) {
	sw.stack = append(sw.stack, cswFrame{
		node: sn,
	})
	switch sn := sn.(type) {
	case *sqllang.Column:
		return sw.visitEnterColumn(ctx, sn)
	case *sqllang.Join:
		return sw.visitEnterJoin(ctx, sn)
	case *sqllang.Select:
		return sw.visitEnterSelect(ctx, sn)
	case *sqllang.Sort:
		return sw.visitEnterSort(ctx, sn)
	case *sqllang.Source:
		return sw.visitEnterSource(ctx, sn)
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitEnterColumn(ctx context.Context, col *sqllang.Column) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.w.WriteString(", "); err != nil {
			return nil, err
		}
	}
	top.columns++
	if err := sw.writeExpr(ctx, col.Expr); err != nil {
		return nil, err
	}
	if err := sw.writeAlias(col.Alias); err != nil {
		return nil, err
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitEnterJoin(ctx context.Context, j *sqllang.Join) (sqllang.Visitor, error) {
	if _, err := sw.w.WriteString("INNER JOIN "); err != nil {
		return nil, err
	}
	if err := sqllang.Walk(ctx, &j.Source, sw); err != nil {
		return nil, err
	}
	if _, err := sw.w.WriteString(" ON "); err != nil {
		return nil, err
	}
	return sw, sw.writeExpr(ctx, j.On)
}

func (sw *commonSQLWriterVisitor) visitEnterSelect(ctx context.Context, sn *sqllang.Select) (sqllang.Visitor, error) {
	if len(sw.stack) > 1 {
		// this is not the top-level statement, so wrap it into
		// parens
		if _, err := sw.w.Write(constBytes[openParenIndex : openParenIndex+1]); err != nil {
			return nil, err
		}
		sw.stack[len(sw.stack)-1].suffix = ")"
	}
	if _, err := sw.w.WriteString("SELECT "); err != nil {
		return nil, err
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitEnterSort(ctx context.Context, sn *sqllang.Sort) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.w.WriteString(", "); err != nil {
			return nil, err
		}
	} else {
		if _, err := sw.w.WriteString("ORDER BY "); err != nil {
			return nil, err
		}
	}
	top.columns++
	if err := sw.writeExpr(ctx, sn.By); err != nil {
		return nil, err
	}
	if sn.Desc {
		if _, err := sw.w.WriteString(" DESC"); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitEnterSource(ctx context.Context, sn *sqllang.Source) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.w.WriteString(" FROM "); err != nil {
			return nil, err
		}
		top.columns = 0
	}
	if len(sn.Table) != 0 {
		if _, err := sw.w.WriteString(sn.Table); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitExit(ctx context.Context) (sqllang.Visitor, error) {
	f := sw.stack[len(sw.stack)-1]
	sw.stack = sw.stack[:len(sw.stack)-1]
	switch sn := f.node.(type) {
	case *sqllang.Source:
		return sw.visitExitSource(ctx, sn)
	}
	if f.suffix != "" {
		if _, err := sw.w.WriteString(f.suffix); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *commonSQLWriterVisitor) visitExitSource(ctx context.Context, sn *sqllang.Source) (sqllang.Visitor, error) {
	if err := sw.writeAlias(sn.Alias); err != nil {
		return nil, err
	}
	return sw, nil
}
