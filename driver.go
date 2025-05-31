package sqlstream

import (
	"context"
	"fmt"

	"github.com/skillian/sqlstream/sqllang"
)

type Driver interface {
	ArgWriterTo

	// ArgLimit returns the SQL statement parameter limit.
	ArgLimit() int
}

// WithSQLServer configures the SQLWriterTo to use SQL Server syntax
// (e.g. `TOP (n)` for DELETEs, SELECTs, and UPDATEs).
func WithSQLServer() interface {
	DBOption
	DBInfoOption
	SQLWriterOption
} {
	return WithSQLLangVisitorWrapper(func(w SmallWriter, inner sqllang.Visitor) (sqllang.Visitor, error) {
		return &sqlServerWriterVisitor{
			w:     w,
			inner: inner,
		}, nil
	})
}

type sqlServerWriterVisitor struct {
	w     SmallWriter
	inner sqllang.Visitor
}

func (vis *sqlServerWriterVisitor) Visit(ctx context.Context, node sqllang.Node) (v sqllang.Visitor, err error) {
	v, err = vis.inner.Visit(ctx, node)
	if err != nil {
		return nil, err
	}
	switch node := node.(type) {
	case *sqllang.Select:
		if node.Limit != nil {
			if _, err = vis.w.WriteString("TOP ("); err != nil {
				return nil, fmt.Errorf(
					"writing TOP: %w", err,
				)
			}
			if _, err = vis.w.WriteString(node.Limit.Text(10)); err != nil {
				return nil, fmt.Errorf(
					"writing number after TOP: %w",
					err,
				)
			}
			if _, err = vis.w.WriteString(") "); err != nil {
				return nil, fmt.Errorf(
					"writing closing ) after TOP: %w",
					err,
				)
			}
		}
	}
	return
}

// WithSQLite3 configures the SQLWriterTo to use SQLite 3 syntax
// (e.g. `LIMIT n` for DELETEs, SELECTs, and UPDATEs).
func WithSQLite3() interface {
	DBOption
	DBInfoOption
	SQLWriterOption
} {
	// TODO: Also handle converting time.Time to some SQLite type
	return WithSQLLangVisitorWrapper(func(w SmallWriter, inner sqllang.Visitor) (sqllang.Visitor, error) {
		return &sqlite3WriterVisitor{
			w:     w,
			inner: inner,
		}, nil
	})
}

type sqlite3WriterVisitor struct {
	w     SmallWriter
	inner sqllang.Visitor
	stack []sqllang.Node
}

func (vis *sqlite3WriterVisitor) Visit(ctx context.Context, node sqllang.Node) (v sqllang.Visitor, err error) {
	v, err = vis.inner.Visit(ctx, node)
	if err != nil {
		return nil, err
	}
	if node != nil {
		vis.stack = append(vis.stack, node)
		return
	} else {
		node = vis.stack[len(vis.stack)-1]
		vis.stack = vis.stack[:len(vis.stack)-1]
	}
	switch node := node.(type) {
	case *sqllang.Select:
		if node.Limit != nil {
			if _, err = vis.w.WriteString("LIMIT "); err != nil {
				return nil, fmt.Errorf(
					"writing LIMIT: %w", err,
				)
			}
			limitString := node.Limit.Text(10)
			if _, err = vis.w.WriteString(limitString); err != nil {
				return nil, fmt.Errorf(
					"writing %q after LIMIT: %w",
					limitString, err,
				)
			}
		}
	}
	return
}
