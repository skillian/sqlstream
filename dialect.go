package sqlstream

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

var (
	MSSQL interface {
		Dialect
	} = mssql{}

	errBadIdent = errors.New("invalid identifier")
)

type Dialect interface {
	Quote(w io.Writer, ident string) (n int, err error)
}

type commonDialect struct{}

var _ interface {
	Dialect
} = commonDialect{}

func (commonDialect) Quote(w io.Writer, ident string) (n int, err error) {
	if strings.IndexByte(ident, '"') != -1 {
		return 0, fmt.Errorf("%w: %q", errBadIdent, ident)
	}
	n, err = w.Write(dquoteBytes)
	if err != nil {
		return
	}
	nn, err := io.WriteString(w, ident)
	n += nn
	if err != nil {
		return
	}
	nn, err = w.Write(dquoteBytes)
	n += nn
	return
}

type mssql struct{ commonDialect }
