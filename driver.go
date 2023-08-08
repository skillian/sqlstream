package sqlstream

import "io"

var (
	// ODBC is a common ODBC driver
	ODBC Driver = odbc{}
)

// Driver contains methods that are specific to the SQL driver being
// used.  For example, parameter placeholders can be independent of the
// RDBMS and/or dialect of SQL (e.g. ODBC vs. MS SQL Server, etc.)
type Driver interface {
	// MakeParameters creates a set of parameters specific
	MakeParameters() Parameters
}

// Parameters is a collection of parameters specific to a SQL statement.
type Parameters interface {
	WriteParameter(w io.Writer, v interface{}) (n int, err error)
}

type odbc struct{}

func (odbc) MakeParameters() Parameters { return odbc{} }
func (odbc) WriteParameter(w io.Writer, v interface{}) (int, error) {
	return w.Write(qmarkBytes)
}
