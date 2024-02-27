package sqlstream

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

type selectTest struct {
	node func(context.Context) sqllang.Node
	odbc sqlWriterToExpect
}

type sqlWriterToExpect struct {
	sql  string
	args []interface{}
}

var testDB = func() *DB {
	sqlDB, err := NewDB(
		WithDriverDialect(ODBC),
	)
	if err != nil {
		panic(err)
	}
	return sqlDB
}()

type TestID struct {
	Value int64
}

type Test struct {
	TestID TestID `sqlstream:"test id,64"`
	Name   string `sqlstream:"name,64"`
}

var selectTests = []selectTest{
	{
		node: func(ctx context.Context) sqllang.Node {
			t := &Test{}
			q := testDB.Query(ctx, t)
			return &sqllang.Select{
				Columns: []sqllang.Column{
					{Expr: 456},
					{Expr: expr.MemOf(q.Var(), t, &t.TestID)},
					{Expr: expr.MemOf(q.Var(), t, &t.Name)},
				},
				From: sqllang.Source{
					Table: "test",
					Alias: "T0",
				},
				Where: sqllang.Where{
					Expr: expr.Eq{
						expr.MemOf(q, t, &t.TestID),
						123,
					},
				},
				OrderBy: []sqllang.Sort{
					{By: expr.MemOf(q, t, &t.TestID), Desc: true},
				},
				Limit: big.NewInt(1),
			}
		},
		odbc: sqlWriterToExpect{
			sql:  `SELECT TOP (1) ?, T0."TestID", T0."Name" FROM "test" WHERE T0."TestID" = ? ORDER BY T0."TestID" DESC;`,
			args: []interface{}{456, 123},
		},
	},
}

func TestSelect(t *testing.T) {
	testSQLWriterTo := func(ctx context.Context, t *testing.T, dd DriverDialect, n sqllang.Node, expect *sqlWriterToExpect) {
		sb := strings.Builder{}
		swt, err := dd.SQLWriter(ctx, &sb)
		if err != nil {
			t.Fatal(err)
		}
		i64, err := swt.WriteSQL(ctx, n)
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
	ctx := context.TODO()
	for i := range selectTests {
		tc := &selectTests[i]
		n := tc.node(ctx)
		t.Run(fmt.Sprintf("selectTest[%d]", i), func(t *testing.T) {
			testSQLWriterTo(ctx, t, ODBC, n, &tc.odbc)
		})
	}
}
