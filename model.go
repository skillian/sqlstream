package sqlstream

import (
	"reflect"
	"strings"
	"unsafe"

	"github.com/skillian/sqlstream/sqllang"
	"github.com/skillian/syng"
)

// Scanner is implemented by `*sql.DB`, `*sql.Conn`, etc. to scan
// results of queries into a slice of pointers to values
type Scanner interface {
	Scan(vs []interface{}) error
}

// fieldsAppender appends pointers to fields into a slice and returns
// the appended slice.
type FieldsAppender interface {
	AppendFields(fs []interface{}) []interface{}
}

// valuesAppender appends values into the given slice.
type ValuesAppender interface {
	AppendValues(vs []interface{}) []interface{}
}

// Model pairs together an underlying model type and the data
// necessary to scan to/from it.
type Model struct {
	V         interface{}
	modelType modelType
}

var _ interface {
	FieldsAppender
	ValuesAppender
} = (*Model)(nil)

// ModelOf creates a model which can be scanned into.
//
// v must be a pointer in order to be scanned into.
func ModelOf(v interface{}) Model {
	return Model{
		V:         v,
		modelType: modelTypeOf(v),
	}
}

func (m Model) AppendFields(fs []interface{}) []interface{} {
	flds := m.modelType.fieldV1s()
	for i := range flds {
		fs = append(fs, flds[i].pointerTo(m.V))
	}
	return fs
}

func (m Model) AppendValues(vs []interface{}) []interface{} {
	flds := m.modelType.fieldV1s()
	for i := range flds {
		vs = append(vs, flds[i].valueOf(m.V))
	}
	return vs
}

func (m Model) Unwrap() interface{} { return m.V }

type anyModelType interface {
	// Name is the name of the model type with spaces.  The DB
	// namer is responsible for coming up with the actual name.
	Name() string

	// anyFields retrieves field descriptors of all the model's
	// fields.
	anyFields() []anyField

	// reflectType gets the reflect.Type of the model
	reflectType() reflect.Type
}

func newAny(mt anyModelType) interface{} {
	if nr, ok := mt.(interface{ newAny() interface{} }); ok {
		return nr.newAny()
	}
	return reflect.New(mt.reflectType()).Interface()
}

type anyModelTypesV1 struct {
	ms   []anyModelType
	fs   []anyField
	rt   reflect.Type
	name string
}

func modelTypesOf(ms []anyModelType) anyModelType {
	ns := make([]string, len(ms))
	sfs := make([]reflect.StructField, len(ms))
	fs := make([]anyField, 0, arbitraryCapacity)
	for i, m := range ms {
		ns[i] = m.Name()
		sfs[i] = reflect.StructField{
			Name: nameString(ns[i], PascalCase),
			Type: m.reflectType(),
		}
		fs = append(fs, m.anyFields()...)
	}
	mts := &anyModelTypesV1{
		ms:   ms,
		fs:   make([]anyField, len(fs)),
		rt:   reflect.StructOf(sfs),
		name: strings.Join(ns, ""),
	}
	copy(mts.fs, fs)
	return mts
}

func (ms *anyModelTypesV1) Name() string              { return ms.name }
func (ms *anyModelTypesV1) anyFields() []anyField     { return ms.fs }
func (ms *anyModelTypesV1) reflectType() reflect.Type { return ms.rt }

type anyField interface {
	Name() string
	valueOfAny(structPtr interface{}) (value interface{})
	pointerToAny(structPtr interface{}) (fieldPointer interface{})
}

type modelType[T any] interface {
	anyModelType
	fieldV1s() []fieldV1[T]
}

var modelTypes = syng.Map[reflect.Type, anyModelType]{}

func modelTypeOf[T any]() modelType[T] {
	var v *T
	key := reflect.TypeOf(v).Elem()
	amt, ok := modelTypes.Load(key)
	if ok {
		return amt.(modelType[T])
	}
	parseTag := func(tag string) (name string, tp sqllang.Type) {
		var ok bool
		name, tag, ok = strings.Cut(tag, ",")
		if !ok {
			return
		}
		var typename string
		typename, tag, ok = strings.Cut(tag, ",")
		if typename != "" {
			panic("TODO: parse types")
		}
		return
	}
	newModelType := func(rt reflect.Type) modelType[T] {
		mt := &modelTypeV1[T]{}
		modelFields := make([]fieldV1[T], 0, rt.NumField())
		if cap(modelFields) == 0 {
			panic("cannot create model type without interface{} fields")
		}
		i := 0
		sf := rt.Field(i)
		if sf.Type == emptyStructType {
			i++
			tv, ok := sf.Tag.Lookup(tag)
			if !ok {
				panic("model empty struct field must have " + tag + " tag")
			}
			mt.name, _ = parseTag(tv)
		}
		if len(mt.name) == 0 {
			mt.name = rt.Name()
		}
		for ; i < cap(modelFields); i++ {
			sf = rt.Field(i)
			modelFields = append(modelFields, fieldV1[T]{
				offset: sf.Offset,
			})
			mf := &modelFields[len(modelFields)-1]
			mf.name, _ = parseTag(sf.Name)
			if len(mf.name) == 0 {
				mf.name = sf.Name
			}
			rv := reflect.NewAt(sf.Type, unsafe.Pointer(&modelTypes) /* Doesn't matter where */)
			x := rv.Interface()
			mf.pointerType = (*((*[2]unsafe.Pointer)(unsafe.Pointer(&x))))[0]
			x = rv.Elem().Interface()
			mf.valueType = (*((*[2]unsafe.Pointer)(unsafe.Pointer(&x))))[0]
			mf.reflectType = sf.Type
		}
		mt.modelFieldV1s = make([]fieldV1[T], len(modelFields))
		copy(mt.modelFieldV1s, modelFields)
		mt.anyModelFields = make([]anyField, len(mt.modelFieldV1s))
		for i := range mt.modelFieldV1s {
			mt.anyModelFields[i] = &mt.modelFieldV1s[i]
		}
		return mt
	}
	mt := newModelType(key)
	amt, ok = modelTypes.LoadOrStore(key, mt)
	if ok {
		return amt.(modelType[T])
	}
	return mt
}

type modelTypeV1 struct {
	modelFieldV1s []field

	// anyModelFields contains the same fields as modelFields but
	// pre-copied into a properly-typed slice.
	anyModelFields []anyField
	rt             reflect.Type
	name           string
}

var _ interface {
	modelType[struct{}]
} = (*modelTypeV1[struct{}])(nil)

func (mt *modelTypeV1[T]) Name() string              { return mt.name }
func (mt *modelTypeV1[T]) anyFields() []anyField     { return mt.anyModelFields }
func (mt *modelTypeV1[T]) fields() []field           { return mt.modelFields }
func (mt *modelTypeV1[T]) newAny() interface{}       { return new(T) }
func (mt *modelTypeV1[T]) reflectType() reflect.Type { return mt.rt }

// field is a field of a structure.
type field struct {
	// offset from the beginning of the parent structure
	offset uintptr

	// valueType is the type of the value within the struct
	valueType unsafe.Pointer

	// pointerType is the type of the pointer to the value within
	// the struct
	pointerType unsafe.Pointer

	// name is the name of the field
	name string

	// reflectType is the `reflec.Type` of the field value.
	reflectType reflect.Type
}

func (f field) Name() string                           { return f.name }
func (f field) valueOf(v unsafe.Pointer) interface{}   { return f.getField(v, f.valueType) }
func (f field) valueOfAny(v interface{}) interface{}   { return f.valueOf() }
func (f field) pointerTo(v unsafe.Pointer) interface{} { return f.getField(v, f.pointerType) }
func (f field) pointerToAny(v interface{}) interface{} { return f.pointerTo(v.(*T)) }

func (f *field) getField(v unsafe.Pointer, pt unsafe.Pointer) (x interface{}) {
	xp := (*[2]unsafe.Pointer)(unsafe.Pointer(&x))
	(*xp) = [...]unsafe.Pointer{pt, unsafe.Add(v, f.offset)}
	return
}
