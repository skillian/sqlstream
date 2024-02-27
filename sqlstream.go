package sqlstream

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/skillian/errors"
	"github.com/skillian/expr"
	"github.com/skillian/logging"
)

const (
	Debug  = true
	Unsafe = true

	arbitraryCapacity   = 8
	maxInt64Base10Bytes = 20 // "-9223372036854775808"
	tag                 = "sqlstream"
)

var (
	logger = logging.GetLogger("github.com/skillian/sqlstream")

	emptyStructType = reflect.TypeOf(struct{}{})

	constBytes = []byte(" !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")
	//                   012 3456789012345678901234 56789012
)

const (
	spaceIndex      = 0
	openParenIndex  = 8
	closeParenIndex = 9
	qmarkIndex      = 21
)

// replaceArgs replaces a slice of args which may contain `exprVar`s
// with values associated with those `exprVar`'s `Var()`s
func replaceArgs(ctx context.Context, args []interface{}) (replaced []interface{}, err error) {
	var vs expr.Values
	replaced = make([]interface{}, len(args))
	for i, arg := range args {
		if ev, ok := arg.(exprVar); ok {
			if vs == nil {
				if vs, err = expr.ValuesFromContext(ctx); err != nil {
					return
				}
			}
			replaced[i], err = vs.Get(ctx, ev.Var)
			if err != nil {
				err = errors.ErrorfWithCause(
					err, "failed to get %v from %v",
					ev.Var, vs,
				)
				return
			}
			continue
		}
		replaced[i] = arg
	}
	return
}

type exprVar struct{ Var expr.Var }

type ifaceData struct {
	Type unsafe.Pointer
	Data unsafe.Pointer
}

func ifaceDataOf(ptrToInterface unsafe.Pointer) *ifaceData {
	return (*ifaceData)(ptrToInterface)
}
