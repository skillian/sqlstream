package sqlstream

import (
	"context"
	"fmt"
	"io"

	"github.com/skillian/sqlstream/sqllang"
)

var MSSQL interface {
	DriverDialect
} = mssqlDriverDialect{}

type mssqlDriverDialect struct{}

func (mssqlDriverDialect) mssqlSQLWriter(ctx context.Context, w io.Writer) (msw mssqlSQLWriter, err error) {
	msw.odbcSQLWriter, err = odbcDriverDialect{}.odbcSQLWriter(ctx, w)
	return
}

func (mssqlDriverDialect) SQLWriter(ctx context.Context, w io.Writer) (sw SQLWriter, err error) {
	sw, err = mssqlDriverDialect{}.mssqlSQLWriter(ctx, w)
	return
}

type mssqlSQLWriter struct{ odbcSQLWriter }

var _ interface {
	SQLWriter
} = (*mssqlSQLWriter)(nil)

func (sw mssqlSQLWriter) WriteSQL(ctx context.Context, sql sqllang.Node) (int64, error) {
	w := &sw.odbcSQLWriter.commonSQLWriter.w
	start := w.written
	err := sqllang.Walk(ctx, sql, mssqlSQLWriterVisitor{odbcSQLWriterVisitor{
		(*commonSQLWriterVisitor)(sw.odbcSQLWriter.commonSQLWriter),
	}})
	return w.written - start, err
}

type mssqlSQLWriterVisitor struct{ odbcSQLWriterVisitor }

func (v mssqlSQLWriterVisitor) Visit(ctx context.Context, sn sqllang.Node) (sqllang.Visitor, error) {
	v2, err := v.odbcSQLWriterVisitor.Visit(ctx, sn)
	if err != nil {
		return nil, err
	}
	if sel, ok := sn.(*sqllang.Select); ok && sel.Limit != nil {
		w := &v.odbcSQLWriterVisitor.commonSQLWriterVisitor.w
		if _, err := fmt.Fprintf(
			w, "TOP(%d) ", sel.Limit,
		); err != nil {
			return nil, fmt.Errorf(
				"failed to format MSSQL TOP (n): %w",
				err,
			)
		}
	}
	return v2, nil
}
