package sqllang_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/skillian/sqlstream/sqllang"
)

type parseTypeTest struct {
	name   string
	expect sqllang.Type
}

var parseTypeTests = []parseTypeTest{
	{"char(16)", sqllang.MakeString(16, sqllang.FixedFlag)},
	{"nchar(16)", sqllang.MakeString(
		16, sqllang.FixedFlag|sqllang.WideFlag,
	)},
	{"varchar(127)", sqllang.MakeString(127, 0)},
	{"nvarchar(127)", sqllang.MakeString(127, sqllang.WideFlag)},
	{"varnchar(127)", sqllang.MakeString(127, sqllang.WideFlag)},
	{"integer", sqllang.Int32Type},
	{"int", sqllang.Int32Type},
	{"bigint", sqllang.Int64Type},
	{"date", sqllang.Time{Prec: 24 * time.Hour}},
	{"time", sqllang.Duration{}},
	{"datetime", sqllang.Time{}},
}

func TestParseType(t *testing.T) {
	for i := range parseTypeTests {
		tc := &parseTypeTests[i]
		t.Run(tc.name, func(t *testing.T) {
			tp, err := sqllang.ParseTypeName(tc.name)
			if err != nil {
				t.Fatal(fmt.Errorf(
					"failed to parse %v: %w",
					tc.name, err,
				))
			}
			if tp != tc.expect {
				t.Fatal(fmt.Errorf(
					"expected\n\t%#v\nactual:\n\t%#v",
					tc.expect, tp,
				))
			}
		})
	}
}
