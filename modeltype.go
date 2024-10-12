package sqlstream

import (
	"fmt"
	"math/bits"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/skillian/errutil"
	"github.com/skillian/sqlstream/sqllang"
	sqlddl "github.com/skillian/sqlstream/sqllang/ddl"
	"github.com/skillian/unsafereflect"

	"github.com/davecgh/go-spew/spew"
)

// modelType holds sqlstream-specific data associated with models
type modelType struct {
	unsafereflectType *unsafereflect.Type

	// flatFieldIndexes[:cap(flatFieldIndexes)] is the full
	// data structure.
	//
	// flatFieldIndexes[:len(flatFieldIndexes)] is the number of
	// fields.
	//
	// flatFieldIndexes[:2*len(flatFieldIndexes)] is a sequence
	// of pairs of field indexes.
	//
	// flatFieldIndexes[2*len(flatFieldIndexes):cap(flatFieldIndexes)]
	// are the ranges of fields.
	//
	// So:
	//
	//	allFieldIndexes := flatFieldIndexes[2*len(flatFieldIndexes):cap(fieldIndexes)]
	//	firstFieldIndexRange := flatFieldIndexes[0:2] // {0 4}
	//	firstFieldIndexes := allFieldIndexes[firstFieldIndexRange[0]:firstFieldIndexRange[1]]
	//	next := model
	//	for i := 0; i < len(firstFieldIndexes); i++ {
	//		mt := modelTypeOf(next)
	//		next = mt.unsafereflectType.UnsafeFieldValue(next, i)
	//	}
	//	// innermost field value:
	//	return next
	//
	flatFieldIndexes []int

	structFields []reflect.StructField

	rawName sqlddl.TableName
	sqlName sqlddl.TableName

	// columnNames[:len(columnNames)] are the rawNames
	// columnNames[len(columnNames):cap(columnNames)] are the sqlNames
	columnNames []string
	columnTypes []sqllang.Type
}

func modelTypeOf(v interface{}) *modelType {
	switch v2 := v.(type) {
	case *modelType:
		logger.Warn2(
			"expected %v(value), not %[1]v(%T)",
			errutil.Caller(0).FuncName,
			v,
		)
		return v2
	case reflect.Type:
		logger.Warn2(
			"expected %v(value), not %[1]v(%v)",
			errutil.Caller(0).FuncName,
			v,
		)
		return modelTypeOfReflectType(v2)
	}
	return modelTypeOfReflectType(reflect.TypeOf(v))
}

var (
	modelTypes sync.Map
)

// splitGoName splits a Go identifier into separate parts (e.g.
// "ObjectName" is yielded as "Object" and "Name") and supports
// acronyms like "ID", "HTTP", etc.
func splitGoName(name string, arg interface{}, fn func(arg interface{}, part string) error) error {
	type wrappedArg struct {
		arg interface{}
		fn  func(arg interface{}, part string) error
		buf []byte
	}
	wrapped := wrappedArg{
		arg: arg,
		fn:  fn,
		buf: make([]byte, 0, arbitraryCapacity),
	}
	return splitGoNameNoAcronyms(name, &wrapped, func(arg interface{}, part string) error {
		wrapped := arg.(*wrappedArg)
		r, n := utf8.DecodeRuneInString(part)
		if len(part) == n /* is single "letter" */ && unicode.IsUpper(r) {
			wrapped.buf = utf8.AppendRune(wrapped.buf, r)
			return nil
		}
		if len(wrapped.buf) > 0 {
			err := wrapped.fn(wrapped.arg, string(wrapped.buf))
			wrapped.buf = wrapped.buf[:0]
			if err != nil {
				return err
			}
		}
		return wrapped.fn(wrapped.arg, part)
	})
}

// splitGoNameNoAcronyms is a simple Go identifier tokenizer.  It takes
// an identifier like ObjectName and calls fn for "Object" and "Name".
// It doesn't support acronyms, so a Go identifier like ObjectID will
// yield "Object", "I", "D".
//
// For acronyms, use splitGoName.
func splitGoNameNoAcronyms(name string, arg interface{}, fn func(arg interface{}, part string) error) error {
	const noStart = -1
	start := noStart
	for i, r := range []rune(name) {
		switch {
		case unicode.IsUpper(r):
			if start != noStart {
				if err := fn(arg, name[start:i]); err != nil {
					return err
				}
			}
			start = i
		}
	}
	if start != noStart {
		return fn(arg, name[start:])
	}
	return nil
}

func modelTypeOfReflectType(rt reflect.Type) *modelType {
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() == reflect.Pointer {
		panic(fmt.Sprintf(
			"cannot create model type from pointer to: %v",
			rt.Name(),
		))
	}
	parseNameFromTag := func(t reflect.StructTag) (rawName, sqlName, after string, ok bool) {
		after, ok = t.Lookup(tagName)
		if !ok {
			return
		}
		rawName, after, ok = strings.Cut(after, ",")
		if !ok {
			return
		}
		sqlName, after, ok = strings.Cut(after, ",")
		return
	}
	createModelType := func(rt reflect.Type) (mt *modelType) {
		// make sure pointer-to-model type is initialized:
		_ = unsafereflect.TypeFromReflectType(reflect.PtrTo(rt))
		urt := unsafereflect.TypeFromReflectType(rt)
		mt = &modelType{
			unsafereflectType: urt,
		}
		sfs := urt.ReflectStructFields()
		fieldIndexes := make([][2]int, 0, 2<<bits.Len(uint(len(sfs))))
		flatFieldIndexes := make([]int, 0, cap(fieldIndexes)*cap(fieldIndexes)/2)
		structFields := make([]reflect.StructField, 0, cap(fieldIndexes))
		columnNames := make([]string, 0, cap(structFields))
		columnTypes := make([]sqllang.Type, 0, cap(structFields))
		for i := range sfs {
			sf := &sfs[i]
			if i == 0 && sf.Name == "_" && sf.Type.Size() == 0 {
				rawName, sqlName, after, ok := parseNameFromTag(sf.Tag)
				mt.rawName.Parse(rawName)
				mt.sqlName.Parse(sqlName)
				// TODO: Anything other than names?
				_, _ = after, ok
				continue
			}
			switch sf.Type.Kind() {
			case reflect.Struct:
				// a "substruct:"
				submodel := modelTypeOfReflectType(sf.Type)
				_ = submodel.iterFields(nil, func(
					f *modelTypeIterField,
				) error {
					ffStartIndex := len(flatFieldIndexes)
					// append the index of the current substruct:
					flatFieldIndexes = append(
						flatFieldIndexes,
						i,
					)
					// append the rest of the substruct's "path":
					flatFieldIndexes = append(
						flatFieldIndexes,
						f.fieldIndex...,
					)
					fieldIndexes = append(
						fieldIndexes,
						[2]int{
							ffStartIndex,
							ffStartIndex + len(flatFieldIndexes),
						},
					)
					structFields = append(structFields, *f.reflectStructField)
					structFields[len(structFields)-1].Offset += sf.Offset
					columnNames = append(columnNames, f.rawName, f.sqlName)
					columnTypes = append(columnTypes, f.sqlType)
					return nil
				})
			default:
				j := len(flatFieldIndexes)
				flatFieldIndexes = append(flatFieldIndexes, i)
				fieldIndexes = append(fieldIndexes, [2]int{j, j + 1})
				structFields = append(structFields, *sf)
				rawName, sqlName, after, ok := parseNameFromTag(sf.Tag)
				columnNames = append(columnNames, rawName, sqlName)
				var columnType sqllang.Type
				if ok {
					var typeName string
					typeName, after, ok = strings.Cut(after, ",")
					if len(typeName) > 0 {
						var err error
						columnType, err = sqllang.ParseTypeName(typeName)
						if err != nil {
							panic(err)
						}
					}
				}
				if columnType == nil {
					columnType = sqllang.TypeFromReflectType(sf.Type)
				}
				columnTypes = append(columnTypes, columnType)
			}
		}
		// reallocate to exactly-fitting capacities to not
		// waste space now that the fields will be immutable.
		mt.flatFieldIndexes = make(
			[]int,
			2*len(fieldIndexes)+len(flatFieldIndexes),
		)
		for i, fi := range fieldIndexes {
			i *= 2
			mt.flatFieldIndexes[i] = fi[0]
			mt.flatFieldIndexes[i+1] = fi[1]
		}
		copy(mt.flatFieldIndexes[2*len(fieldIndexes):], flatFieldIndexes)
		// "encode" the length into the len part of the slice:
		mt.flatFieldIndexes = mt.flatFieldIndexes[:len(fieldIndexes)]
		if mt.rawName == (sqlddl.TableName{}) {
			parts := make([]string, 0, arbitraryCapacity)
			splitGoName(rt.Name(), &parts, func(arg interface{}, part string) error {
				parts := arg.(*[]string)
				*parts = append(*parts, part)
				return nil
			})
			mt.rawName.Parse(strings.Join(parts, " "))
		}
		mt.structFields = make([]reflect.StructField, len(structFields))
		copy(mt.structFields, structFields)
		// columnNames =    [col 0 rawName, col 0 sqlName, col 1 rawName, col 1 sqlName]
		// mt.columnNames = [col 0 rawName, col 1 rawName, col 0 sqlName, col 1 sqlName]
		mt.columnNames = make([]string, len(columnNames))
		{
			length := len(columnNames) / 2
			for i := range columnNames[:length] {
				j := i * 2
				mt.columnNames[i] = columnNames[j]
				mt.columnNames[length+i] = columnNames[j+1]
			}
			mt.columnNames = mt.columnNames[:length]
		}
		mt.columnTypes = make([]sqllang.Type, len(columnTypes))
		copy(mt.columnTypes, columnTypes)
		logger.Verbose1("%v", spew.Sdump(mt))
		return
	}
	key := interface{}(rt)
	if v, loaded := modelTypes.Load(key); loaded {
		return v.(*modelType)
	}
	mt := createModelType(rt)
	if v, loaded := modelTypes.LoadOrStore(key, mt); loaded {
		return v.(*modelType)
	}
	return mt
}

func (mt *modelType) AppendFieldPointers(args []interface{}, v interface{}) []interface{} {
	return mt.appendFields(args, v, (*unsafereflect.Type).FieldPointer)
}

func (mt *modelType) AppendUnsafeFieldValues(args []interface{}, v interface{}) []interface{} {
	return mt.appendFields(args, v, (*unsafereflect.Type).UnsafeFieldValue)
}

// ReflectStructFields gets the struct fields that will be scanned into
func (mt *modelType) ReflectStructFields() []reflect.StructField { return mt.structFields }

func (mt *modelType) appendFields(args []interface{}, v interface{}, f func(*unsafereflect.Type, interface{}, int) interface{}) []interface{} {
	_ = mt.iterFields(v, func(mtif *modelTypeIterField) error {
		v := mtif.arg
		for i, urt := range mtif.unsafereflectTypePath[:len(mtif.unsafereflectTypePath)-1] {
			v = urt.UnsafeFieldValue(v, mtif.fieldIndex[i])
		}
		args = append(
			args,
			f(
				mtif.unsafereflectTypePath[len(mtif.unsafereflectTypePath)-1],
				v,
				mtif.fieldIndex[len(mtif.fieldIndex)-1],
			),
		)
		return nil
	})
	return args
}

type modelTypeIterField struct {
	// arg passed to iterFields
	arg interface{}

	// unsafereflectTypePath is the sequence of
	unsafereflectTypePath []*unsafereflect.Type
	index                 int
	fieldIndex            []int
	reflectStructField    *reflect.StructField
	rawName               string
	sqlName               string
	sqlType               sqllang.Type
}

func (mt *modelType) iterFields(arg interface{}, eachField func(f *modelTypeIterField) error) error {
	type modelTypeIterFieldWithCache struct {
		modelTypeIterField
		unsafereflectTypePathCache [arbitraryCapacity]*unsafereflect.Type
	}
	mtifwc := modelTypeIterFieldWithCache{
		modelTypeIterField: modelTypeIterField{
			arg: arg,
		},
	}
	mtifwc.modelTypeIterField.unsafereflectTypePath = mtifwc.unsafereflectTypePathCache[:1]
	mtif := &mtifwc.modelTypeIterField
	mtif.unsafereflectTypePath[0] = mt.unsafereflectType
	len2 := len(mt.flatFieldIndexes) * 2
	fieldIndexPairs := mt.flatFieldIndexes[:len2]
	allFieldIndexes := mt.flatFieldIndexes[len2:cap(mt.flatFieldIndexes)]
	for mtif.index = range mt.flatFieldIndexes {
		i := mtif.index * 2
		mtif.fieldIndex = allFieldIndexes[fieldIndexPairs[i]:fieldIndexPairs[i+1]]
		{
			mtif.unsafereflectTypePath = mtif.unsafereflectTypePath[:1]
			for _, j := range mtif.fieldIndex[:len(mtif.fieldIndex)-1] {
				mtif.unsafereflectTypePath = append(
					mtif.unsafereflectTypePath,
					mtif.unsafereflectTypePath[len(mtif.unsafereflectTypePath)-1].FieldType(j),
				)
			}
		}
		mtif.reflectStructField = &mt.unsafereflectType.ReflectStructFields()[mtif.fieldIndex[len(mtif.fieldIndex)-1]]
		mtif.rawName = mt.columnNames[mtif.index]
		mtif.sqlName = mt.columnNames[:cap(mt.columnNames)][len(mt.columnNames)+mtif.index]
		mtif.sqlType = mt.columnTypes[mtif.index]
		if err := eachField(mtif); err != nil {
			return err
		}
	}
	return nil
}

func (mt *modelType) columnRawNames() []string { return mt.columnNames[:len(mt.columnNames)] }
func (mt *modelType) columnSQLNames() []string {
	return mt.columnNames[len(mt.columnNames):cap(mt.columnNames)]
}
