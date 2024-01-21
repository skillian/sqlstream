package sqlstream_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/skillian/expr"
	"github.com/skillian/sqlstream"
	"github.com/skillian/sqlstream/sqllang"
)

type selectTest struct {
	node func(context.Context) sqllang.Node
	odbc sqlWriterToExpect
}

var testDB = sqlstream.NewDB(
	sqlstream.WithDriverDialect(sqlstream.ODBC),
)

type TestID struct {
	Value int64
}

type Test struct {
	TestID TestID
	Name   string
}

var selectTests = []selectTest{
	{
		node: func(ctx context.Context) sqllang.Node {
			q := sqlstream.Query[Test](ctx, testDB)
			m := sqlstream.ModelOf(&Test{})
			return &sqllang.Select{
				Columns: []sqllang.Column{
					sqllang.Column{Expr: expr.MemOf(q.Var(), t, &t.TestID)},
					sqllang.Column{Expr: expr.MemOf(q.Var(), t, &t.Name)},
				},
				From: sqllang.Source{
					Table: _,
				},
			}
		},
		odbc: sqlWriterToExpect{
			sql:  "(? = ?)",
			args: []interface{}{1, 1},
		},
	},
	{
		expr: expr.Ne{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? <> ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Eq{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? = ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Lt{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? < ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Gt{2, 1},
		odbc: sqlWriterToExpect{
			sql:  "(? > ?)",
			args: []interface{}{2, 1},
		}},
	{
		expr: expr.Ge{3, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? >= ?)",
			args: []interface{}{3, 2},
		}},
	{
		expr: expr.Le{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? <= ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Add{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? + ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Sub{2, 1},
		odbc: sqlWriterToExpect{
			sql:  "(? - ?)",
			args: []interface{}{2, 1},
		}},
	{
		expr: expr.Mul{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? * ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Div{1, 2},
		odbc: sqlWriterToExpect{
			sql:  "(? / ?)",
			args: []interface{}{1, 2},
		}},
	{
		expr: expr.Eq{expr.Add{1, 2}, expr.Sub{3, expr.Mul{4, expr.Div{10, 2}}}},
		odbc: sqlWriterToExpect{
			sql:  "((? + ?) = (? - (? * (? / ?))))",
			args: []interface{}{1, 2, 3, 4, 10, 2},
		}},
}

func TestSelect(t *testing.T) {
	testSQLWriterTo := func(ctx context.Context, t *testing.T, dd sqlstream.DriverDialect, e expr.Expr, expect *exprTestExpect) {
		sb := strings.Builder{}
		swt, err := dd.SQLWriterTo(ctx)
		if err != nil {
			t.Fatal(err)
		}
		i64, err := swt.WriteSQLTo(ctx, &sb, e)
		if err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		if int64(sb.Len()) != i64 {
			t.Errorf("expected %d bytes written, actual: %d", i64, sb.Len())
		}
		s := sb.String()
		if s != expect.sql || !reflect.DeepEqual(swt.Args(), expect.args) {
			t.Errorf(
				"expected:\n\t%s\nexpected args:\n\t"+
					"%#v\nactual:\n\t%s\nactual args:\n\t"+
					"%#v\n",
				expect.sql, expect.args, s, swt.Args(),
			)
		}
	}
	for i := range exprTests {
		tc := &exprTests[i]
		t.Run(fmt.Sprint(tc.expr), func(t *testing.T) {
			ctx := context.TODO()
			testSQLWriterTo(ctx, t, sqlstream.ODBC, tc.expr, &tc.odbc)
		})
	}
}
