package sqlstream

import (
	"context"
	"io"

	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

var ODBC interface {
	DriverDialect
} = odbcDriverDialectV1{}

type odbcDriverDialectV1 struct{}

var _ interface {
	DriverDialect
} = odbcDriverDialectV1{}

func (odbcDriverDialectV1) SQLWriterTo(ctx context.Context) (SQLWriterTo, error) {
	return &odbcSQLWriterToV1{}, nil
}

type odbcSQLWriterToV1 struct {
	exprWriterToV1
}

var _ interface {
	SQLWriterTo
} = (*odbcSQLWriterToV1)(nil)

func (swt *odbcSQLWriterToV1) WriteSQLTo(ctx context.Context, w io.Writer, sql sqllang.Node) (int64, error) {
	sw := &odbcSQLWriterV1{
		odbcSQLWriterToV1:  swt,
		smallWriterCounter: smallWriterCounterOf(w),
	}
	start := sw.smallWriterCounter.written
	err := sqllang.Walk(ctx, sql, sw)
	return sw.smallWriterCounter.written - start, err
}

type odbcSQLWriterV1 struct {
	*odbcSQLWriterToV1
	*smallWriterCounter
	stack []oswV1Frame
}

var _ interface {
	sqllang.Visitor
} = (*odbcSQLWriterV1)(nil)

type oswV1Frame struct {
	node    sqllang.Node
	suffix  string
	columns int
}

func (sw *odbcSQLWriterV1) writeAlias(alias string) error {
	if alias == "" {
		return nil
	}
	if _, err := sw.Write(constBytes[spaceIndex : spaceIndex+1]); err != nil {
		return err
	}
	_, err := sw.WriteString(alias)
	return err
}

func (sw *odbcSQLWriterV1) writeExpr(ctx context.Context, e expr.Expr) error {
	_, err := sw.odbcSQLWriterToV1.WriteExprTo(
		ctx, sw.smallWriterCounter, e,
	)
	return err
}

func (sw *odbcSQLWriterV1) top(skip int) *oswV1Frame {
	return &sw.stack[len(sw.stack)-1-skip]
}

func (sw *odbcSQLWriterV1) Visit(ctx context.Context, sn sqllang.Node) (sqllang.Visitor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sn != nil {
		return sw.visitEnter(ctx, sn)
	}
	return sw.visitExit(ctx)
}

func (sw *odbcSQLWriterV1) visitEnter(ctx context.Context, sn sqllang.Node) (sqllang.Visitor, error) {
	sw.stack = append(sw.stack, oswV1Frame{
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

func (sw *odbcSQLWriterV1) visitEnterColumn(ctx context.Context, col *sqllang.Column) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.smallWriterCounter.WriteString(",\n\t"); err != nil {
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

func (sw *odbcSQLWriterV1) visitEnterJoin(ctx context.Context, j *sqllang.Join) (sqllang.Visitor, error) {
	if _, err := sw.smallWriterCounter.WriteString("INNER JOIN "); err != nil {
		return nil, err
	}
	if err := sqllang.Walk(ctx, &j.Source, sw); err != nil {
		return nil, err
	}
	if _, err := sw.smallWriterCounter.WriteString(" ON "); err != nil {
		return nil, err
	}
	return sw, sw.writeExpr(ctx, j.On)
}

func (sw *odbcSQLWriterV1) visitEnterSelect(ctx context.Context, sn *sqllang.Select) (sqllang.Visitor, error) {
	if len(sw.stack) > 1 {
		// this is not the top-level statement, so wrap it into
		// parens
		if _, err := sw.Write(constBytes[openParenIndex : openParenIndex+1]); err != nil {
			return nil, err
		}
		sw.stack[len(sw.stack)-1].suffix = ")"
	}
	if _, err := sw.WriteString("SELECT "); err != nil {
		return nil, err
	}
	return sw, nil
}

func (sw *odbcSQLWriterV1) visitEnterSort(ctx context.Context, sn *sqllang.Sort) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.WriteString(", "); err != nil {
			return nil, err
		}
	}
	top.columns++
	if err := sw.writeExpr(ctx, sn.By); err != nil {
		return nil, err
	}
	if sn.Desc {
		if _, err := sw.WriteString(" DESC"); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *odbcSQLWriterV1) visitEnterSource(ctx context.Context, sn *sqllang.Source) (sqllang.Visitor, error) {
	top := sw.top(1)
	if top.columns > 0 {
		if _, err := sw.WriteString(" FROM "); err != nil {
			return nil, err
		}
		top.columns = 0
	}
	if len(sn.Table) != 0 {
		if _, err := sw.WriteString(sn.Table); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *odbcSQLWriterV1) visitExit(ctx context.Context) (sqllang.Visitor, error) {
	f := sw.stack[len(sw.stack)-1]
	sw.stack = sw.stack[:len(sw.stack)-1]
	switch sn := f.node.(type) {
	case *sqllang.Source:
		return sw.visitExitSource(ctx, sn)
	}
	if f.suffix != "" {
		if _, err := sw.WriteString(f.suffix); err != nil {
			return nil, err
		}
	}
	return sw, nil
}

func (sw *odbcSQLWriterV1) visitExitSource(ctx context.Context, sn *sqllang.Source) (sqllang.Visitor, error) {
	if err := sw.writeAlias(sn.Alias); err != nil {
		return nil, err
	}
	return sw, nil
}
