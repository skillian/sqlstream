package sqlstream

import (
	"fmt"
	"strings"
	"testing"

	"github.com/skillian/logging"
)

type testThingID struct {
	Value int64
}

type testThing struct {
	ThingID   testThingID
	ThingName string
}

func TestModelType(t *testing.T) {
	defer logging.TestingHandler(logger, t, logging.HandlerLevel(logging.VerboseLevel))()
	th := &testThing{
		ThingID:   testThingID{Value: 123},
		ThingName: "testThingName",
	}
	mt := modelTypeOf(th)
	fs := mt.AppendFieldPointers(make([]interface{}, 0, 2), th)
	assert(t, len(fs) == 2, "len(fs) == 2")
	assert(t, fs[0] == &th.ThingID.Value, "fs[0] == &th.ThingID.Value")
	assert(t, fs[1] == &th.ThingName, "fs[1] == &th.ThingName")
	fs = mt.AppendUnsafeFieldValues(fs[:0], th)
	assert(t, len(fs) == 2, "len(fs) == 2")
	assert(t, fs[0] == int64(123), "fs[0] == int64(123)")
	assert(t, fs[1] == "testThingName", "fs[1] == \"testThingName\"")
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
	parts := new([]string)
	err := splitGoName("HTTPServer", parts, func(arg interface{}, part string) error {
		parts := arg.(*[]string)
		*parts = append(*parts, part)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(*parts) != 2 {
		t.Fatalf(
			"expected length %v, actual %v",
			2, len(*parts),
		)
	}
	if (*parts)[0] != "HTTP" {
		t.Fatal("expected first part to be 'HTTP'")
	}
	if (*parts)[1] != "Server" {
		t.Fatal("expected first part to be 'Server'")
	}
}
