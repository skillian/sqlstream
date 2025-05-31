package sqlstream

import (
	"context"
	"strings"
	"testing"

	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/logging"
	_ "modernc.org/sqlite"
)

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
	{"\r \t \nhello \t\r\nworld", "hello", "world", true},
}

func TestStringsCutTrimSpaceTests(t *testing.T) {
	for i := range stringsCutTrimSpaceTests {
		tc := &stringsCutTrimSpaceTests[i]
		t.Run(tc.input, func(t *testing.T) {
			prefix, suffix, ok := stringsCutTrimSpace(tc.input)
			if prefix != tc.prefix || suffix != tc.suffix || ok != tc.ok {
				t.Fatalf(
					"expected:\n\t%#v\nactual:\n\t%#v",
					[]any{tc.prefix, tc.suffix, tc.ok},
					[]any{prefix, suffix, ok},
				)
			}
		})
	}
}

// getOrCreateTestDB gets an existing or creates a new test in-memory
// database.  If key is empty, a unique in-memory database is returned.
// Otherwise, any calls to this function that request the same key
// will get the same database
func getOrCreateTestDB(t *testing.T, key string, options ...DBOption) *DB {
	t.Helper()
	connectionString := ":memory:"
	if len(key) > 0 {
		connectionString = strings.Join([]string{
			"file:",
			key,
			"?mode=memory&cache=shared",
		}, "")
	}
	{
		options2 := make([]DBOption, len(options)+2)
		options2[0] = WithSQLOpen("sqlite", connectionString)
		options2[1] = WithQueryPrepareAfterCount(1)
		copy(options2[2:], options)
		options = options2
	}
	db, err := NewDB(options...)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

const testDBKey = "testdb"

func TestQuery(t *testing.T) {
	cleanupTestingHandler := logging.TestingHandler(
		logger,
		t,
		logging.HandlerFormatter(logging.GoFormatter{}),
		logging.HandlerLevel(logging.EverythingLevel),
	)
	defer cleanupTestingHandler()
	dbi, err := NewDBInfo(
		WithSQLServer(),
	)
	if err != nil {
		t.Fatalf("new DB info: %v", err)
	}
	if err != nil {
		t.Fatalf("new SQL writer: %v", err)
	}
	ctx := dbi.AddToContext(context.Background())
	db := getOrCreateTestDB(t, testDBKey, WithDBInfo(dbi))
	ctx = db.AddToContext(ctx)
	ctx, endTx, err := WithTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { endTx(err) }()
	if err = testDBSaveQuery(t, ctx, db); err != nil {
		t.Error(err)
	}
}

func testDBSaveQuery(t *testing.T, ctx context.Context, db *DB) error {
	h := &testHand{
		HandID: testHandID{456},
	}
	f := &testFinger{
		FingerID:   testFingerID{123},
		FingerName: "foo",
		HandID:     h.HandID,
	}
	fq := Query(ctx, f)
	fq = stream.Filter(ctx, fq, expr.Eq{
		expr.MemOf(fq.Var(), f, &f.FingerID),
		123,
	})
	hq := Query(ctx, h)
	fq = stream.Join(ctx, fq, hq, expr.Eq{
		expr.MemOf(fq.Var(), f, &f.HandID),
		expr.MemOf(hq.Var(), h, &h.HandID),
	}, expr.Tuple{
		fq.Var(),
		hq.Var(),
	})
	h2, f2 := &testHand{}, &testFinger{}
	*h2, *h = *h, testHand{}
	*f2, *f = *f, testFinger{}
	ctx, vs := expr.GetOrAddValuesToContext(ctx)
	vs.Set(ctx, fq.Var(), expr.Tuple{f, h})
	return stream.Each(ctx, fq, nil, func(
		ctx context.Context,
		s stream.Stream,
		state, value interface{},
	) (err error) {
		return nil
	})
}

type loggerQuerier struct {
	logger *logging.Logger
	level  logging.Level
}

var _ sqlQuerier = (*loggerQuerier)(nil)

func (lq *loggerQuerier) QueryContext(ctx context.Context, s string, args ...interface{}) (sqlRows, error) {
	lq.logger.Log2(
		lq.level, "SQL string:\n\t%s\nargs:\n\t%#v",
		s, args,
	)
	return emptyRows{}, nil
}
