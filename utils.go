package sqlstream

import (
	"fmt"
	"math/bits"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/skillian/logging"
)

const (
	Unsafe = false

	arbitraryCapacity = 8

	tagName = "sqlstream"
)

var (
	logger = logging.GetLogger(
		"sqlstream",
		logging.LoggerLevel(logging.EverythingLevel),
	)
)

func unwrap(v interface{}, target interface{}) (isUnwrapped bool) {
	const errorFormat = "unwrap(%#v, %#v): %s"
	targetValue := reflect.ValueOf(target)
	if !targetValue.IsValid() {
		logger.Warn3(errorFormat, v, target, "target is not valid")
		return false
	}
	if targetValue.Kind() != reflect.Ptr {
		logger.Warn3(errorFormat, v, target, "target is not a pointer")
		return false
	}
	if targetValue.IsNil() {
		logger.Warn3(errorFormat, v, target, "target is nil")
		return false
	}
	targetElementType := targetValue.Type().Elem()
	valueValue := reflect.ValueOf(v)
	for {
		if !valueValue.IsValid() {
			return false
		}
		if valueValue.Type().ConvertibleTo(targetElementType) {
			targetValue.Elem().Set(valueValue.Convert(targetElementType))
			return true
		}
		m, ok := valueValue.Type().MethodByName("Unwrap")
		if !ok {
			return false
		}
		if m.Type.NumIn() != 1 && m.Type.NumOut() != 1 {
			return false
		}
		valueValue = m.Func.Call([]reflect.Value{valueValue})[0]
	}
}

type atomicString struct {
	unsafeValue string
}

var atomicLoadInt, atomicStoreInt = func() (func(*int) int, func(*int, int)) {
	switch bits.UintSize {
	case 32:
		return func(p *int) int {
				return int(atomic.LoadInt32((*int32)(unsafe.Pointer(p))))
			}, func(p *int, v int) {
				atomic.StoreInt32((*int32)(unsafe.Pointer(p)), int32(v))
			}
	case 64:
		return func(p *int) int {
				return int(atomic.LoadInt64((*int64)(unsafe.Pointer(p))))
			}, func(p *int, v int) {
				atomic.StoreInt64((*int64)(unsafe.Pointer(p)), int64(v))
			}
	default:
		panic(fmt.Sprintf("unknown UintSize: %d", bits.UintSize))
	}
}()

func (p *atomicString) LoadOrCreate(arg interface{}, create func(interface{}) string) string {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&p.unsafeValue))
	var value string
	valueSH := (*reflect.StringHeader)(unsafe.Pointer(&value))
	if valueSH.Data = atomic.LoadUintptr(&sh.Data); valueSH.Data == 0 {
		value = create(arg)
		if atomic.CompareAndSwapUintptr(&sh.Data, 0, valueSH.Data) {
			atomicStoreInt(&sh.Len, len(value))
			return value
		}
		valueSH.Data = atomic.LoadUintptr(&sh.Data)
	}
	i := 0
	const maxIterations = 1000
	for ; i < maxIterations; i++ {
		valueSH.Len = atomicLoadInt(&sh.Len)
		if valueSH.Len > 0 {
			break
		}
	}
	if i == maxIterations {
		panic("length was not written")
	}
	return value
}

// func writeByte64(w io.Writer, b byte, currentN int64) (n int64, err error) {
// 	i, err := writeByte(w, b, 0)
// 	return currentN + int64(i), err
// }

// func writeByte(w io.Writer, b byte, currentN int) (n int, err error) {
// 	if bw, ok := w.(io.ByteWriter); ok {
// 		if err = bw.WriteByte(b); err == nil {
// 			n++
// 		}
// 	} else {
// 		var bs []byte
// 		if Unsafe {
// 			bs = ((*[1]byte)(unsafe.Pointer(&b)))[:]
// 		} else {
// 			bs = []byte{b}
// 		}
// 		n, err = w.Write(bs)
// 	}
// 	n += currentN
// 	return
// }

// func writeString64(w io.Writer, s string, currentN int64) (n int64, err error) {
// 	i, err := writeString(w, s, 0)
// 	return currentN + int64(i), err
// }

// func writeString(w io.Writer, s string, currentN int) (n int, err error) {
// 	if sw, ok := w.(io.StringWriter); ok {
// 		n, err = sw.WriteString(s)
// 	} else if Unsafe {
// 		sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
// 		var b []byte
// 		bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
// 		bh.Data = sh.Data
// 		bh.Len = sh.Len
// 		bh.Cap = sh.Len
// 		n, err = w.Write(b)
// 	} else {
// 		n, err = w.Write([]byte(s))
// 	}
// 	n += currentN
// 	return
// }
