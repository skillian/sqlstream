package sqlstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Driver contains methods that are specific to the SQL driver being
// used.  For example, parameter placeholders can be independent of the
// RDBMS and/or dialect of SQL (e.g. ODBC vs. MS SQL Server, etc.)
type Driver interface {
	// MakeParameters creates a set of parameters specific to the
	// driver
	MakeParameters() Parameters
}

// Parameters is a collection of parameter values specific to a SQL
// statement.
type Parameters interface {
	// WriteParameterTo writes a parameter into the parameter args.
	// Implementations with named parameters might reuse the
	// same parameter.
	WriteParameterTo(w io.Writer, v interface{}) (n int64, err error)

	// Args are passed to ExecuteContext, QueryContext, etc.
	Args() []interface{}
}

type Dialect interface {
	Quote(w io.Writer, ident string) (n int, err error)
}

type commonDialect struct{}

var _ interface {
	Dialect
} = commonDialect{}

var errBadIdent = errors.New("invalid identifier")

func (commonDialect) Quote(w io.Writer, ident string) (n int, err error) {
	if strings.IndexByte(ident, '"') != -1 {
		return 0, fmt.Errorf("%w: %q", errBadIdent, ident)
	}
	n, err = w.Write(dquoteBytes)
	if err != nil {
		return
	}
	nn, err := io.WriteString(w, ident)
	n += nn
	if err != nil {
		return
	}
	nn, err = w.Write(dquoteBytes)
	n += nn
	return
}

type DriverDialect interface {
	Driver
	Dialect
	SQLWriter(ctx context.Context, w io.Writer) (SQLWriter, error)
}

type driverDialect1 struct {
	driver  Driver
	dialect Dialect
}

func (dd driverDialect1) commonSQLWriter(ctx context.Context, w io.Writer) (*commonSQLWriter, error) {
	sw := &commonSQLWriter{}
	sw.init(w, dd.driver.MakeParameters(), dd.dialect)
	return sw, nil
}

func (dd driverDialect1) SQLWriter(ctx context.Context, w io.Writer) (SQLWriter, error) {
	return dd.commonSQLWriter(ctx, w)
}
