package sqlstream

import (
	"fmt"
	"strings"
	"testing"

	"github.com/skillian/logging"
)

type testFingerID struct {
	Value int64
}

type testFinger struct {
	FingerID   testFingerID
	FingerName string
	HandID     testHandID
}

type testHandID struct {
	Value int64
}

type testHand struct {
	HandID   testHandID
	HandName string
}

func TestModelType(t *testing.T) {
	defer logging.TestingHandler(logger, t, logging.HandlerLevel(logging.VerboseLevel))()
	th := &testFinger{
		FingerID:   testFingerID{Value: 123},
		FingerName: "testThingName",
		HandID:     testHandID{Value: 456},
	}
	mt := modelTypeOf(th)
	fs := mt.AppendFieldPointers(make([]interface{}, 0, 3), th)
	assert(t, len(fs) == 3, "len(fs) == 3")
	assert(t, fs[0] == &th.FingerID.Value, "fs[0] == &th.FingerID.Value")
	assert(t, fs[1] == &th.FingerName, "fs[1] == &th.FingerName")
	assert(t, fs[2] == &th.HandID.Value, "fs[2] == &th.HandID.Value")
	fs = mt.AppendUnsafeFieldValues(fs[:0], th)
	assert(t, len(fs) == 3, "len(fs) == 3")
	assert(t, fs[0] == int64(123), "fs[0] == int64(123)")
	assert(t, fs[1] == "testThingName", "fs[1] == \"testThingName\"")
	assert(t, fs[2] == int64(456), "fs[2] == int64(456)")
}

func assert(t *testing.T, result bool, repr string, args ...interface{}) {
	t.Helper()
	if !result {
		sb := strings.Builder{}
		sb.WriteString("assertion failed: ")
		fmt.Fprintf(&sb, repr, args...)
		t.Fatal(sb.String())
	}
}

func TestGoSplitName(t *testing.T) {
	type testSplitName struct {
		name  string
		parts []string
	}
	testSplitNames := []testSplitName{
		{"HTTPServer", []string{"HTTP", "Server"}},
		{"testName", []string{"test", "Name"}},
	}
	parts := new([]string)
	for _, tc := range testSplitNames {
		*parts = (*parts)[:0]
		err := splitGoName(tc.name, parts, func(arg interface{}, part string) error {
			parts := arg.(*[]string)
			*parts = append(*parts, part)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(*parts) != len(tc.parts) {
			t.Fatalf(
				"expected length %v, actual %v",
				len(tc.parts), len(*parts),
			)
		}
		for i, part := range *parts {
			if part != tc.parts[i] {
				t.Fatalf(
					"expected part %d to be %q, not %q",
					i, tc.parts[i], part,
				)
			}
		}
	}
}
