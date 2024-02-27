package sqlstream

import (
	"context"
	"io"

	"github.com/skillian/sqlstream/sqllang"
)

var ODBC interface {
	DriverDialect
} = odbcDriverDialect{}

type odbcDriverDialect struct{ commonDialect }

func (dd odbcDriverDialect) MakeParameters() Parameters {
	return &odbcParameters{args: make([]interface{}, 0, arbitraryCapacity)}
}

type odbcParameters struct {
	args []interface{}
}

var _ interface {
	Parameters
} = (*odbcParameters)(nil)

func (ps *odbcParameters) Args() []interface{} { return ps.args }

func (ps *odbcParameters) WriteParameterTo(w io.Writer, v interface{}) (int64, error) {
	_, err := w.Write(constBytes[qmarkIndex : qmarkIndex+1])
	if err != nil {
		return 0, err
	}
	ps.args = append(ps.args, v)
	return 1, nil
}

func (dd odbcDriverDialect) odbcSQLWriter(ctx context.Context, w io.Writer) (osw odbcSQLWriter, err error) {
	osw.commonSQLWriter, err = driverDialect1{
		driver:  dd,
		dialect: dd,
	}.commonSQLWriter(ctx, w)
	return
}

func (dd odbcDriverDialect) SQLWriter(ctx context.Context, w io.Writer) (sw SQLWriter, err error) {
	sw, err = dd.odbcSQLWriter(ctx, w)
	return
}

type odbcSQLWriter struct{ *commonSQLWriter }

var _ interface {
	SQLWriter
} = odbcSQLWriter{}

func (osw odbcSQLWriter) WriteSQL(ctx context.Context, sql sqllang.Node) (int64, error) {
	start := osw.commonSQLWriter.w.written
	err := sqllang.Walk(ctx, sql, odbcSQLWriterVisitor{(*commonSQLWriterVisitor)(osw.commonSQLWriter)})
	return osw.commonSQLWriter.w.written - start, err
}

func (swt odbcSQLWriter) Args() []interface{} { return swt.commonSQLWriter.Args() }

type odbcSQLWriterVisitor struct{ *commonSQLWriterVisitor }
