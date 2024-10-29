package sqlstream

import (
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	// SnakeCase translates "Hello World" to "Hello_World"
	SnakeCase NameWriterTo = snakeCase{}
	// SnakeCaseLower translates "Hello World" to "hello_world"
	SnakeCaseLower NameWriterTo = snakeCase{lowerCase: true}
	// SnakeCaseUpper translates "Hello World" to "HELLO_WORLD"
	SnakeCaseUpper NameWriterTo = snakeCase{upperCase: true}
	// CamelCase translates "Hello World" to "helloWorld"
	CamelCase NameWriterTo = oneWord{capFirst: false, lowerCase: true}
	// PascalCase translates "Hello World" to "HelloWorld"
	PascalCase NameWriterTo = oneWord{capFirst: true, lowerCase: true}

	unicodeLetterOrNumber = [...]*unicode.RangeTable{
		unicode.Letter,
		unicode.Number,
	}
)

func foreachIdent(state interface{}, name string, f func(state interface{}, ident string) error) error {
	const noStart = -1
	start := noStart
	for i, r := range name {
		if !unicode.In(r, unicodeLetterOrNumber[:]...) {
			if start != noStart {
				if err := f(state, name[start:i]); err != nil {
					return err
				}
				start = noStart
			}
		} else if start == noStart {
			start = i
		}
	}
	if start != noStart {
		if err := f(state, name[start:]); err != nil {
			return err
		}
	}
	return nil
}

// NapeWriterTo takes in a name as a string and writes its "translated"
// version into the given writer.
type NameWriterTo interface {
	WriteNameTo(w io.Writer, name string) (int, error)
}

// nameString uses a name writer to write the given name (with spaces)
// into an identifier.
func nameString(name string, nwt NameWriterTo) string {
	sb := strings.Builder{}
	if _, err := nwt.WriteNameTo(&sb, name); err != nil {
		panic(err)
	}
	return sb.String()
}

// NameWritersTo has a different NameWriterTo for each kind of object
type NameWritersTo struct {
	Column NameWriterTo
	Table  NameWriterTo
	Schema NameWriterTo
}

func (nwt *NameWritersTo) init() {
	if nwt.Column == nil {
		nwt.Column = SnakeCaseLower
	}
	if nwt.Table == nil {
		nwt.Table = SnakeCaseLower
	}
	if nwt.Schema == nil {
		nwt.Schema = SnakeCaseLower
	}
}

type snakeCase struct {
	lowerCase bool
	upperCase bool
}

func (sc snakeCase) WriteNameTo(w io.Writer, name string) (int, error) {
	type data struct {
		w  io.Writer
		i  int
		n  int
		bs [1]byte
		sc snakeCase
	}
	d := data{w: w, bs: [...]byte{'_'}, sc: sc}
	err := foreachIdent(&d, name, func(anyData interface{}, ident string) error {
		d := anyData.(*data)
		if d.i > 0 {
			n, err := d.w.Write(d.bs[:])
			d.n += n
			if err != nil {
				return err
			}
		}
		d.i++
		if d.sc.lowerCase {
			ident = strings.ToLower(ident)
		} else if d.sc.upperCase {
			ident = strings.ToUpper(ident)
		}
		n, err := io.WriteString(d.w, ident)
		d.n += n
		return err
	})
	return d.n, err
}

type oneWord struct {
	capFirst  bool
	lowerCase bool
}

func (ow oneWord) WriteNameTo(w io.Writer, name string) (int, error) {
	type data struct {
		sw SmallWriter
		i  int
		n  int
		ow oneWord
	}
	d := data{sw: SmallWriterOf(w), ow: ow}
	err := foreachIdent(&d, name, func(anyData interface{}, ident string) error {
		d := anyData.(*data)
		r, i := utf8.DecodeRuneInString(ident)
		ident = ident[i:]
		if d.i > 0 || (d.i == 0 && d.ow.capFirst) {
			r = unicode.ToUpper(r)
		}
		d.i++
		n, err := d.sw.WriteRune(r)
		d.n += n
		if err != nil {
			return err
		}
		if d.ow.lowerCase {
			for _, r := range ident {
				n, err = d.sw.WriteRune(unicode.ToLower(r))
				d.n += n
				if err != nil {
					return err
				}
			}
			return nil
		}
		n, err = io.WriteString(d.sw, ident)
		d.n += n
		return err
	})
	return d.n, err
}
