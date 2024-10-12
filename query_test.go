package sqlstream

import "testing"

type stringsCutTrimSpaceTest struct {
	input  string
	prefix string
	suffix string
	ok     bool
}

var stringsCutTrimSpaceTests = []stringsCutTrimSpaceTest{
	{"helloworld", "helloworld", "", false},
	{"helloworld\t ", "helloworld", "", true},
	{"hello \t\r\nworld", "hello", "world", true},
}

func TestStringsCutTrimSpaceTests(t *testing.T) {
	for i := range stringsCutTrimSpaceTests {
		tc := &stringsCutTrimSpaceTests[i]
		t.Run(tc.input, func(t *testing.T) {
			prefix, suffix, ok := stringsCutTrimSpace(tc.input)
			if prefix != tc.prefix || suffix != tc.suffix || ok != tc.ok {
				t.Fatalf(
					"expected:\n\t%#v\nactual:\n\t%#v",
					[]interface{}{tc.prefix, tc.suffix, tc.ok},
					[]interface{}{prefix, suffix, ok},
				)
			}
		})
	}
}
