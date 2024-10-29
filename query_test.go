package sqlstream

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/skillian/ctxutil"
	"github.com/skillian/expr"
	"github.com/skillian/expr/stream"
	"github.com/skillian/logging"
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

// getOrCreateTestDB gets an existing or creates a new test in-memory
// database.  If key is empty, a unique in-memory database is returned.
// Otherwise, any calls to this function that request the same key
// will get the same database
func getOrCreateTestDB(t *testing.T, key string) *DB {
	t.Helper()
	connectionString := ":memory:"
	if len(key) > 0 {
		connectionString = strings.Join([]string{
			"file:",
			key,
			"?mode=memory&cache=shared",
		}, "")
	}
	db, err := NewDB(
		WithSQLOpen("sqlite3", connectionString),
		WithQueryPrepareAfterCount(1),
	)
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
	db := getOrCreateTestDB(t, testDBKey)
	ctx, vs := expr.GetOrAddValuesToContext(context.Background())
	ctx = ctxutil.WithValue(
		ctx,
		(*sqlPreparer)(nil),
		&loggerPreparer{
			db:     db,
			logger: logger,
			level:  logging.EverythingLevel,
		},
	)
	m := &testThing{
		ThingID:   testThingID{123},
		ThingName: "foo",
	}
	// db.Save(ctx, []interface{}{m})
	q := db.Query(ctx, m)
	q = stream.Filter(ctx, q, expr.Eq{expr.MemOf(q.Var(), m, &m.ThingID), 123})
	vs.Set(ctx, q.Var(), m)
	st, err := q.Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if err := st.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
	}
}

type loggerPreparer struct {
	db     *DB
	logger *logging.Logger
	level  logging.Level
}

var _ sqlPreparer = (*loggerPreparer)(nil)

func (lp *loggerPreparer) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	lp.logger.Log2(lp.level, "db: %v, query:\n%v", lp.db, query)
	return lp.db.db.PrepareContext(ctx, query)
}
