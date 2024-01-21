package sqlstream

import (
	"context"
	"fmt"

	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/syng"
)

type query interface {
	source() query
	stream.Streamer
	stream.Filterer
	expr.Var
}

func tableOfQuery(q query) (t anyTable, tableQuery query) {
	for ; q != nil; q = q.source() {
		switch q := q.(type) {
		case *tableQueryV1:
			return q.table, q
		}
	}
	return nil, nil
}

type queryV1 struct {
	once syng.Once
	sr   stream.Streamer
}

type tableQueryV1 struct {
	queryV1
	db    *DB
	table anyTable
}

func tableQueryV1Of(q query) *tableQueryV1 {
	for q != nil {
		if tq, ok := q.(*tableQueryV1); ok {
			return tq
		}
		q = q.source()
	}
	return nil
}

var _ interface {
	query
} = (*tableQueryV1)(nil)

func (q *tableQueryV1) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return &filterQueryV1{from: q, where: e}, nil
}

func (q *tableQueryV1) Join(ctx context.Context, inner stream.Streamer, when, then expr.Expr) (stream.Streamer, error) {
	if q2, ok := inner.(query); ok {
		if db2 := tableQueryV1Of(q2).db; db2 != q.db {
			return nil, fmt.Errorf("%w: %v != %v", errDBMismatch, q.db, db2)
		}
		return &joinQueryV1{from: q, to: q2, on: when}, nil
	}
	return stream.NewLocalJoiner(ctx, q, inner, when, then)
}

func (q *tableQueryV1) Stream(ctx context.Context) (stream.Stream, error) {
	type state struct {
		q   *tableQueryV1
		ctx context.Context
	}
	syng.DoOnce(&q.queryV1.once, state{q, ctx}, func(st state) {
		st.q.queryV1.sr = st.q.db.queryStreamer(st.ctx, st.q)
	})
	return q.queryV1.sr.Stream(ctx)
}

func (q *tableQueryV1) Var() expr.Var { return q }

func (q *tableQueryV1) source() query { return nil }

type filterQueryV1 struct {
	queryV1
	from  query
	where expr.Expr
}

var _ interface {
	query
} = (*filterQueryV1)(nil)

func (q *filterQueryV1) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return &filterQueryV1{from: q, where: e}, nil
}

func (q *filterQueryV1) Stream(ctx context.Context) (stream.Stream, error) {
	type state struct {
		q   *filterQueryV1
		ctx context.Context
	}
	syng.DoOnce(&q.queryV1.once, state{q, ctx}, func(st state) {
		st.q.queryV1.sr = tableQueryV1Of(st.q).db.queryStreamer(st.ctx, st.q)
	})
	return q.queryV1.sr.Stream(ctx)
}

func (q *filterQueryV1) Var() expr.Var { return q.from.Var() }

func (q *filterQueryV1) source() query { return q.from }

type joinQueryV1 struct {
	queryV1
	from query
	to   query
	on   expr.Expr
	proj expr.Expr
}

var _ interface {
	query
} = (*joinQueryV1)(nil)

func (q *joinQueryV1) Filter(ctx context.Context, e expr.Expr) (stream.Streamer, error) {
	return &filterQueryV1{from: q, where: e}, nil
}

func (q *joinQueryV1) Stream(ctx context.Context) (stream.Stream, error) {
	type state struct {
		q   *joinQueryV1
		ctx context.Context
	}
	syng.DoOnce(&q.queryV1.once, state{q, ctx}, func(st state) {
		st.q.queryV1.sr = tableQueryV1Of(st.q).db.queryStreamer(st.ctx, st.q)
	})
	return q.queryV1.sr.Stream(ctx)
}

func (q *joinQueryV1) Var() expr.Var { return q.from.Var() }

func (q *joinQueryV1) source() query { return q.from }
