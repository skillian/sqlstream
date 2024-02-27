package sqlstream

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
)

type query interface {
	source() query
	stream.Streamer
	stream.Filterer
	expr.Var
}

type queryCommon struct {
	mu                   sync.Mutex
	unsafeStreamer       stream.Streamer
	streamerFactoryState interface{}
	streamerFactoryFunc  func(context.Context, interface{}) stream.Streamer
}

func (qc *queryCommon) init(state interface{}, fn func(context.Context, interface{}) stream.Streamer) {
	qc.streamerFactoryState = state
	qc.streamerFactoryFunc = fn
}

func (qc *queryCommon) getStreamer(ctx context.Context) stream.Streamer {
	id := ifaceDataOf(unsafe.Pointer(&qc.unsafeStreamer))
	for {
		if atomic.LoadPointer(&id.Data) != nil {
			return qc.unsafeStreamer
		}
		sr := qc.streamerFactoryFunc(ctx, qc.streamerFactoryState)
		srData := ifaceDataOf(unsafe.Pointer(&sr))
		if atomic.CompareAndSwapPointer(
			&id.Type,
			nil,
			srData.Type,
		) {
			atomic.StorePointer(
				&id.Data,
				srData.Data,
			)
			return qc.unsafeStreamer
		}
	}
}

type tableQuery struct {
	queryCommon
	db    *DB
	table *table
}

func newTableQuery(db *DB, t *table) (tq *tableQuery) {
	tq = &tableQuery{}
	tq.init(db, t)
	return
}

func (tq *tableQuery) init(db *DB, t *table) {
	tq.queryCommon.init(tq, func(ctx context.Context, anyQuery interface{}) stream.Streamer {
		tq := anyQuery.(*tableQuery)
		return tq.db.queryStreamer(ctx, tq)
	})
	tq.db = db
	tq.table = t
}

func tableQueryOf(q query) *tableQuery {
	for q != nil {
		if tq, ok := q.(*tableQuery); ok {
			return tq
		}
		q = q.source()
	}
	return nil
}

var _ interface {
	query
} = (*tableQuery)(nil)

func (tq *tableQuery) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return newFilterQuery(tq, e), nil
}

func (q *tableQuery) Join(ctx context.Context, inner stream.Streamer, when, then expr.Expr) (stream.Streamer, error) {
	if q2, ok := inner.(query); ok {
		if db2 := tableQueryOf(q2).db; db2 != q.db {
			return nil, errors.Errorf(
				"%w: %v != %v",
				errDBMismatch, q.db, db2,
			)
		}
		return newJoinQuery(q, q2, when, then), nil
	}
	return stream.NewLocalJoiner(ctx, q, inner, when, then)
}

func (q *tableQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.queryCommon.getStreamer(ctx).Stream(ctx)
}

func (q *tableQuery) Var() expr.Var { return q }

func (q *tableQuery) source() query { return nil }

type filterQuery struct {
	queryCommon
	from  query
	where expr.Expr
}

func newFilterQuery(from query, where expr.Expr) (fq *filterQuery) {
	fq = &filterQuery{}
	fq.init(from, where)
	return
}

func (fq *filterQuery) init(from query, where expr.Expr) {
	fq.init(fq, func(ctx context.Context, anyQuery interface{}) stream.Streamer {
		fq := anyQuery.(*filterQuery)
		return tableQueryOf(fq).db.queryStreamer(ctx, fq)
	})
	fq.from = from
	fq.where = where
	return
}

var _ interface {
	query
} = (*filterQuery)(nil)

func (q *filterQuery) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return newFilterQuery(q, e), nil
}

func (q *filterQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.queryCommon.getStreamer(ctx).Stream(ctx)
}

func (q *filterQuery) Var() expr.Var { return q.from.Var() }

func (q *filterQuery) source() query { return q.from }

type joinQuery struct {
	queryCommon
	from query
	to   query
	on   expr.Expr
	proj expr.Expr
}

func newJoinQuery(from query, to query, on expr.Expr, proj expr.Expr) (jq *joinQuery) {
	jq = &joinQuery{}
	jq.init(from, to, on, proj)
	return
}

func (jq *joinQuery) init(from query, to query, on expr.Expr, proj expr.Expr) {
	jq.queryCommon.init(jq, func(ctx context.Context, anyQuery interface{}) stream.Streamer {
		jq := anyQuery.(*joinQuery)
		return tableQueryOf(jq).db.queryStreamer(ctx, jq)
	})
}

var _ interface {
	query
} = (*joinQuery)(nil)

func (q *joinQuery) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return newFilterQuery(q, e), nil
}

func (q *joinQuery) Stream(ctx context.Context) (stream.Stream, error) {
	return q.queryCommon.getStreamer(ctx).Stream(ctx)
}

func (q *joinQuery) Var() expr.Var { return q.from.Var() }

func (q *joinQuery) source() query { return q.from }
