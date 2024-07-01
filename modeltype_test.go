package sqlstream

import (
	"fmt"
	"strings"
	"testing"
)

type testThingID struct {
	Value int64
}

type testThing struct {
	ThingID   testThingID
	ThingName string
}

func TestModelType(t *testing.T) {
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
