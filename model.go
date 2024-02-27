package sqlstream

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/skillian/expr"
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
	modelType *modelType
	V         interface{}
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
	flds := m.modelType.fields
	for i := range flds {
		fs = append(fs, flds[i].pointerTo(m.V))
	}
	return fs
}

func (m Model) AppendValues(vs []interface{}) []interface{} {
	flds := m.modelType.fields
	for i := range flds {
		vs = append(vs, flds[i].valueOf(m.V))
	}
	return vs
}

func (m Model) Unwrap() interface{} { return m.V }

func newModel(ctx context.Context, mt *modelType) (m Model, err error) {
	v, err := mt.newFn(mt, ctx)
	if err != nil {
		return
	}
	m.modelType = mt
	m.V = v
	return
}

var (
	modelTypeReflectType    = reflect.TypeOf((*modelType)(nil)).Elem()
	reflectTypeByModelTypes = sync.Map{} // [...]*modelType -> reflect.Type
)

func modelTypesOf(ms []*modelType) (mts *modelType) {
	arr := reflect.NewAt(
		reflect.ArrayOf(len(ms), modelTypeReflectType),
		unsafe.Pointer(&ms[0]),
	).Elem().Interface()
	if v, ok := reflectTypeByModelTypes.Load(arr); ok {
		return modelTypeOfType(v.(reflect.Type), nil)
	}
	ns := make([]string, len(ms))
	sfs := make([]reflect.StructField, len(ms))
	fieldCount := 0
	for i, m := range ms {
		ns[i] = m.name
		sfs[i] = reflect.StructField{
			Name: nameString(ns[i], PascalCase),
			Type: m.reflectType,
		}
		fieldCount += len(m.fields)
	}
	mts = &modelType{
		fields:      make([]field, 0, fieldCount),
		reflectType: reflect.StructOf(sfs),
		name:        strings.Join(ns, ""),
		components:  ms,
	}
	for i, m := range ms {
		sf := &sfs[i]
		start := len(mts.fields)
		mts.fields = append(mts.fields, m.fields...)
		for j := range mts.fields[start:] {
			mts.fields[start+j].offset += sf.Offset
		}
	}
	if v, ok := reflectTypeByModelTypes.LoadOrStore(arr, mts.reflectType); ok {
		return modelTypeOfType(v.(reflect.Type), nil)
	}
	modelTypePtrsByReflectType.Store(mts.reflectType, mts)
	reflectTypeByModelTypes.Store(arr, mts.reflectType)
	return mts
}

var modelTypePtrsByReflectType = sync.Map{} // reflect.Type -> *modelType

type modelTypeOption func(mt *modelType) error

func modelTypeOf(v interface{}, options ...modelTypeOption) *modelType {
	switch v := v.(type) {
	case Model:
		return v.modelType
	case *Model:
		return v.modelType
	}
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		panic(fmt.Sprint("Cannot get model type of non-struct: ", v))
	}
	return modelTypeOfType(rt, options...)
}

func modelTypeOfType(rt reflect.Type, options ...modelTypeOption) *modelType {
	key := (interface{})(rt)
	amt, ok := modelTypePtrsByReflectType.Load(key)
	if ok {
		return amt.(*modelType)
	}
	parseTag := func(tag string) (name string, ft fieldTag) {
		var ok bool
		name, tag, ok = strings.Cut(tag, ",")
		if !ok {
			return
		}
		var valueString string
		if valueString, tag, ok = strings.Cut(tag, ","); !ok {
			return
		}
		valueString = strings.TrimSpace(valueString)
		if valueString != "" {
			v, err := strconv.ParseInt(valueString, 10, 64)
			if err != nil {
				panic(err)
			}
			ft.scale = v
		}
		if valueString, tag, ok = strings.Cut(tag, ","); !ok {
			return
		}
		valueString = strings.TrimSpace(valueString)
		if valueString != "" {
			v, err := strconv.ParseInt(valueString, 10, 64)
			if err != nil {
				panic(err)
			}
			ft.prec = v
		}
		if valueString, tag, ok = strings.Cut(tag, ","); !ok {
			return
		}
		valueString = strings.TrimSpace(valueString)
		if valueString != "" {
			v, err := strconv.ParseBool(valueString)
			if err != nil {
				panic(err)
			}
			ft.fixed = v
		}
		return
	}
	newModelType := func(rt reflect.Type, options []modelTypeOption) (mt *modelType) {
		mt = &modelType{}
		reflectStructFields := expr.ReflectStructFieldsOfType(rt)
		modelFields := make([]field, 0, rt.NumField())
		if cap(modelFields) == 0 {
			panic("cannot create model type without interface{} fields")
		}
		i := 0
		sf := &reflectStructFields[i]
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
			sf = &reflectStructFields[i]
			modelFields = append(modelFields, field{
				offset: sf.Offset,
			})
			mf := &modelFields[len(modelFields)-1]
			mf.name, mf.tag = parseTag(sf.Tag.Get(PkgName))
			if len(mf.name) == 0 {
				mf.name = sf.Name
			}
			rv := reflect.NewAt(sf.Type, unsafe.Pointer(&modelTypePtrsByReflectType) /* Doesn't matter where */)
			x := rv.Interface()
			mf.pointerType = ifaceDataOf(unsafe.Pointer(&x)).Type
			x = rv.Elem().Interface()
			mf.valueType = ifaceDataOf(unsafe.Pointer(&x)).Type
			mf.structField = sf
		}
		mt.fields = make([]field, len(modelFields))
		copy(mt.fields, modelFields)
		for _, opt := range options {
			if err := opt(mt); err != nil {
				panic(err)
			}
		}
		if mt.newFn == nil {
			mt.newFn = (*modelType).defaultNewFunc
		}
		return
	}
	mt := newModelType(rt, options)

	amt, ok = modelTypePtrsByReflectType.LoadOrStore(key, mt)
	if ok {
		return amt.(*modelType)
	}
	return mt
}

type newModelFunc func(*modelType, context.Context) (interface{}, error)

type modelType struct {
	fields      []field
	newFn       newModelFunc
	reflectType reflect.Type
	name        string
	components  []*modelType
}

func (mt *modelType) defaultNewFunc(_ context.Context) (interface{}, error) {
	return reflect.New(mt.reflectType).Interface(), nil
}

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

	structField *reflect.StructField

	// tag parsed from the struct's field tag
	tag fieldTag
}

type fieldTag struct {
	scale int64
	prec  int64
	fixed bool
}

func (f *field) getField(v unsafe.Pointer, pt unsafe.Pointer) (x interface{}) {
	xp := (*[2]unsafe.Pointer)(unsafe.Pointer(&x))
	(*xp) = [...]unsafe.Pointer{pt, unsafe.Add(v, f.offset)}
	return
}
func (f *field) valueOf(v interface{}) interface{} {
	return f.valueOf(unsafe.Pointer(reflect.ValueOf(v).Pointer()))
}
func (f *field) pointerTo(v interface{}) interface{} {
	return f.pointerTo(unsafe.Pointer(reflect.ValueOf(v).Pointer()))
}
func (f *field) unsafePointerTo(v unsafe.Pointer) interface{} { return f.getField(v, f.pointerType) }
func (f *field) unsafeValueOf(v unsafe.Pointer) interface{}   { return f.getField(v, f.valueType) }
