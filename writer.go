package sqlstream

import (
	"io"
	"reflect"
	"unicode/utf8"
	"unsafe"
)

func tryFlush(v interface{}) (flushed bool, err error) {
	fr, ok := unwrapTo[interface{ Flush() error }](v)
	if !ok {
		return
	}
	return true, fr.Flush()
}

func unwrapTo[T any](v interface{}) (unwrapped T, isUnwrapped bool) {
	for {
		unwrapped, isUnwrapped = v.(T)
		if !isUnwrapped {
			v, isUnwrapped = unwrap(v)
			if !isUnwrapped {
				return
			}
			continue
		}
		return
	}
}

func unwrap(v interface{}) (unwrapped interface{}, isUnwrapped bool) {
	ur, ok := v.(interface{ Unwrap() interface{} })
	if !ok {
		return
	}
	return ur.Unwrap(), true
}

type runeWriter interface{ WriteRune(rune) (int, error) }

// smallWriter extends io.Writer to provide useful methods for writing
// small units of data.
type smallWriter interface {
	io.Writer
	io.ByteWriter
	runeWriter
	io.StringWriter
}

func smallWriterOf(w io.Writer) smallWriter {
	if sw, ok := w.(smallWriter); ok {
		return sw
	}
	sw := &smallWriterWrapper{w: w, enc: utf8.EncodeRune}
	var ok bool
	if sw.bw, ok = unwrapTo[io.ByteWriter](w); ok {
		sw.writeByte = func(sw *smallWriterWrapper, b byte) error {
			return sw.bw.WriteByte(b)
		}
	} else {
		sw.writeByte = func(sw *smallWriterWrapper, b byte) error {
			sw.buf[0] = b
			_, err := sw.Write(sw.buf[:1])
			return err
		}
	}
	if sw.rw, ok = unwrapTo[runeWriter](w); ok {
		sw.writeRune = func(sw *smallWriterWrapper, r rune) (int, error) {
			return sw.rw.WriteRune(r)
		}
	} else {
		sw.writeRune = func(sw *smallWriterWrapper, r rune) (int, error) {
			i := sw.enc(sw.buf[:], r)
			return sw.Write(sw.buf[:i])
		}
	}
	if sw.sw, ok = unwrapTo[io.StringWriter](w); ok {
		sw.writeString = func(sw *smallWriterWrapper, s string) (int, error) {
			return sw.sw.WriteString(s)
		}
	} else {
		sw.writeString = func(sw *smallWriterWrapper, s string) (int, error) {
			var bs []byte
			if Unsafe {
				// Assume the writer doesn't preserve the byte buffer
				// or write into it.
				sl := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
				st := (*reflect.StringHeader)(unsafe.Pointer(&s))
				sl.Data = st.Data
				sl.Cap = st.Len
				sl.Len = st.Len
			} else {
				bs = []byte(s)
			}
			return sw.Write(bs)
		}
	}
	return sw
}

type smallWriterWrapper struct {
	// Writer is the underlying writer that the smallWriter wraps.
	w io.Writer

	bw          io.ByteWriter
	writeByte   func(*smallWriterWrapper, byte) error
	sw          io.StringWriter
	writeString func(*smallWriterWrapper, string) (int, error)
	rw          runeWriter
	writeRune   func(*smallWriterWrapper, rune) (int, error)

	// enc encodes runes into bytes that are written to Writer.
	enc func([]byte, rune) int

	buf [6]byte // should be just 4, per RFC3629, but just in case
}

func (sw *smallWriterWrapper) Unwrap() interface{} { return sw.w }

func (sw *smallWriterWrapper) Write(bs []byte) (n int, err error) {
	n, err = sw.w.Write(bs)
	return
}

func (sw *smallWriterWrapper) WriteByte(b byte) (err error) {
	err = sw.writeByte(sw, b)
	return
}

func (sw *smallWriterWrapper) WriteRune(r rune) (n int, err error) {
	n, err = sw.writeRune(sw, r)
	return
}

func (sw *smallWriterWrapper) WriteString(s string) (n int, err error) {
	n, err = sw.writeString(sw, s)
	return
}

type smallWriterCounter struct {
	smallWriter
	written int64
}

func smallWriterCounterOf(w io.Writer) *smallWriterCounter {
	if swc, ok := w.(*smallWriterCounter); ok {
		return swc
	}
	return &smallWriterCounter{smallWriter: smallWriterOf(w)}
}

var _ interface {
	smallWriter
} = (*smallWriterCounter)(nil)

func (sw *smallWriterCounter) Unwrap() interface{} { return sw.smallWriter }

func (sw *smallWriterCounter) Write(b []byte) (n int, err error) {
	n, err = sw.smallWriter.Write(b)
	sw.written += int64(n)
	return
}

func (sw *smallWriterCounter) WriteByte(b byte) (err error) {
	err = sw.smallWriter.WriteByte(b)
	if err == nil {
		sw.written++
	}
	return
}

func (sw *smallWriterCounter) WriteRune(r rune) (n int, err error) {
	n, err = sw.smallWriter.WriteRune(r)
	sw.written += int64(n)
	return
}

func (sw *smallWriterCounter) WriteString(s string) (n int, err error) {
	n, err = sw.smallWriter.WriteString(s)
	sw.written += int64(n)
	return
}
