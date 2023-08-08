package sqlstream

import (
	"bytes"
	"fmt"

	"github.com/skillian/ctxutil"
	"github.com/skillian/expr"
	"github.com/skillian/sqlstream/sqllang"
)

type selectWriter struct {
	buf         bytes.Buffer
	ew          exprWriter
	columnIndex int
	flags       selectWriterFlags
}

type selectWriterFlags uint32

const (
	selectWroteFrom selectWriterFlags = 1 << iota
)

func (w *selectWriter) init(ps Parameters) {
	w.ew.init(ps)
}

func (w *selectWriter) reset() {
	w.buf.Reset()
	w.ew.reset()
	w.columnIndex = 0
	w.flags = 0
}

func (w *selectWriter) ws(s string) *selectWriter {
	if _, err := w.buf.WriteString(s); err != nil {
		panic(fmt.Sprintf(
			"%T.WriteString returned error: %v",
			w.buf, err,
		))
	}
	return w
}

func (w *selectWriter) VisitSelect(s *sqllang.Select, v sqllang.Visitor) (interface{}, error) {
	cv, ok := v.(interface {
		VisitColumn(expr.Expr, sqllang.Visitor) (expr.Expr, error)
	})
	if !ok {
		cv = w
	}
	sv, ok := v.(interface {
		VisitSource(*sqllang.Source, sqllang.Visitor) (interface{}, error)
	})
	w.ws("SELECT\n")
	for i, e := range s.Columns {
		if i > 0 {
			w.ws(",\n")
		}
		w.ws("\t")
		if _, err := cv.VisitColumn(e, v); err != nil {
			return nil, err
		}
	}
	w.ws("\nFROM\n\t")
	if _, err := 
}

func (w *selectWriter) VisitColumn(e expr.Expr, v sqllang.Visitor) (expr.Expr, error) {
	if w.columnIndex > 0 {
		w.ws(", ")
	}
	ctx := ctxutil.Background()
	_, err := w.ew.writeExpr(ctx, &w.buf, e)
	return e, err
}

func (w *selectWriter) String() string { return w.buf.String() }
