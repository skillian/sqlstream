package sqlstream

import (
	"context"
	"io"

	"github.com/skillian/sqlstream/sqllang"
)

// DriverDialect generates SQL specific to a particular driver and/or
// dialect of SQL.
type DriverDialect interface {
	// SQLWriter returns a SQLWriterTo that is used to generate a
	// SQL statement.  SQLWriterTo may be called concurrently but
	// the each returned SQLWriterTo will not be used
	// concurrently.  If the implementation uses a single shared
	// instance, it needs to manage the synchronization itself.
	SQLWriterTo(ctx context.Context) (SQLWriterTo, error)
}

// SQLWriter writes values as SQL into an `io.Writer`.
type SQLWriterTo interface {
	ExprWriterTo

	// WriteSQLTo writes a SQL node to a writer.
	WriteSQLTo(ctx context.Context, w io.Writer, sql sqllang.Node) (int64, error)
}
