package sqllang

import (
	"database/sql"
	"math"
	"math/big"
	"math/bits"
	"reflect"
	"time"
)

// Type is an "RDBMS-agnostic" way to represent a SQL data type.
type Type interface {
	// New creates a pointer to a value that can be scanned into
	New() interface{}

	// ReflectType returns the reflect.Type of this type
	ReflectType() reflect.Type
}

// TypeOf gets a default type for a given value
func TypeOf(v interface{}) Type {
	rt := reflect.TypeOf(v)
	if st, ok := typeLookups.reflectTypeToSQLType[rt]; ok {
		return st
	}
	switch rt.Kind() {
	case reflect.Pointer:
		return Nullable{TypeOf(reflect.Zero(rt.Elem()).Interface())}
	}
	return NullBinaryType
}

var (
	BoolType     Type = boolType{}
	NullBoolType Type = Nullable{BoolType}

	IntType       Type = Int{SignBits: -bits.UintSize}
	Int8Type      Type = Int{SignBits: -8}
	Int16Type     Type = Int{SignBits: -16}
	NullInt16Type Type = Nullable{Int16Type}
	Int32Type     Type = Int{SignBits: -32}
	NullInt32Type Type = Nullable{Int32Type}
	Int64Type     Type = Int{SignBits: -64}
	NullInt64Type Type = Nullable{Int64Type}

	UintType     Type = Int{SignBits: bits.UintSize}
	Uint8Type    Type = Int{SignBits: 8}
	NullByteType Type = Nullable{Uint8Type}
	Uint16Type   Type = Int{SignBits: 16}
	Uint32Type   Type = Int{SignBits: 32}
	Uint64Type   Type = Int{SignBits: 64}

	Float32Type     Type = Float{Mantissa: 24}
	Float64Type     Type = Float{Mantissa: 53}
	NullFloat64Type Type = Nullable{Float64Type}

	StringType     Type = String{}
	NullStringType Type = Nullable{StringType}
	TimeType       Type = Time{}
	NullTimeType   Type = Nullable{TimeType}
	BinaryType     Type = Binary{}
	NullBinaryType Type = Nullable{BinaryType}

	BigIntType   Type = Int{SignBits: math.MinInt64}
	BigFloatType Type = Float{^uint(0)}
	BigRatType   Type = Decimal{^uint(0), ^uint(0)}

	boolReflectType = reflect.TypeOf((*bool)(nil)).Elem()

	intType   = reflect.TypeOf((*int)(nil)).Elem()
	int8Type  = reflect.TypeOf((*int8)(nil)).Elem()
	int16Type = reflect.TypeOf((*int16)(nil)).Elem()
	int32Type = reflect.TypeOf((*int32)(nil)).Elem()
	int64Type = reflect.TypeOf((*int64)(nil)).Elem()
	//uintType   = reflect.TypeOf((*uint)(nil)).Elem()
	uint8Type  = reflect.TypeOf((*uint8)(nil)).Elem()
	uint16Type = reflect.TypeOf((*uint16)(nil)).Elem()
	uint32Type = reflect.TypeOf((*uint32)(nil)).Elem()
	uint64Type = reflect.TypeOf((*uint64)(nil)).Elem()

	float32Type = reflect.TypeOf((*float32)(nil)).Elem()
	float64Type = reflect.TypeOf((*float64)(nil)).Elem()

	stringType = reflect.TypeOf((*string)(nil)).Elem()
	timeType   = reflect.TypeOf((*time.Time)(nil)).Elem()

	bytesType = reflect.TypeOf((*[]byte)(nil)).Elem()

	bigIntType   = reflect.TypeOf((*big.Int)(nil))
	bigFloatType = reflect.TypeOf((*big.Float)(nil))
	bigRatType   = reflect.TypeOf((*big.Rat)(nil))

	typeLookups = func() (lookups struct {
		reflectTypeToSQLType map[reflect.Type]Type
		sqlTypeToReflectType map[Type]reflect.Type
	}) {
		lookups.reflectTypeToSQLType = map[reflect.Type]Type{}
		lookups.sqlTypeToReflectType = map[Type]reflect.Type{}
		for _, p := range []struct {
			reflectType reflect.Type
			sqlType     Type
		}{
			{intType, IntType},
			{int8Type, Int8Type},
			{int16Type, Int16Type},
			{int32Type, Int32Type},
			//{uintType, UintType},
			{uint8Type, Uint8Type},
			{uint16Type, Uint16Type},
			{uint32Type, Uint32Type},
			{float32Type, Float32Type},
			{float64Type, Float64Type},
			{bigIntType, BigIntType},
			{bigFloatType, BigFloatType},
			{bigRatType, BigRatType},
			{timeType, TimeType},
			{stringType, StringType},
			{nullBoolType, NullBoolType},
			{nullByteType, NullByteType},
			{nullInt16Type, NullInt16Type},
			{nullInt32Type, NullInt32Type},
			{nullInt64Type, NullInt64Type},
			{nullFloat64Type, NullFloat64Type},
			{nullStringType, NullStringType},
			{nullTimeType, NullTimeType},
		} {
			lookups.reflectTypeToSQLType[p.reflectType] = p.sqlType
			lookups.sqlTypeToReflectType[p.sqlType] = p.reflectType
		}
		return
	}()
)

type boolType struct{}

func (boolType) New() interface{}          { return new(bool) }
func (boolType) ReflectType() reflect.Type { return boolReflectType }

// Decimal is a decimal precision type.
//
// Current implementation detail is to use `*big.Rat` to make sure we don't
// lose accuracy or precision.
type Decimal struct {
	Scale, Precision uint
}

func (t Decimal) New() interface{}          { return &big.Rat{} }
func (t Decimal) ReflectType() reflect.Type { return bigRatType }

// Float indicates a binary floating point value
type Float struct {
	// Mantissa is the number of bits to hold significant figures
	Mantissa uint
}

func (t Float) New() interface{} {
	switch {
	case t.Mantissa <= 24:
		return new(float32)
	case t.Mantissa <= 53:
		return new(float64)
	}
	return &big.Float{}
}

func (t Float) ReflectType() reflect.Type {
	switch {
	case t.Mantissa <= 24:
		return float32Type
	case t.Mantissa <= 53:
		return float64Type
	}
	return bigFloatType
}

// Int indicates an integer data type.
type Int struct {
	// SignBits is negative for signed integer types and positive for
	// unsigned integer types.
	SignBits int
}

func (t Int) New() interface{} {
	if t.SignBits == 0 {
		return new(int)
	}
	switch {
	// TODO: Should negatives' checks be <= instead of < ?
	case t.SignBits < -64:
		return &big.Int{}
	case t.SignBits < -32:
		return new(int64)
	case t.SignBits < -16:
		return new(int32)
	case t.SignBits < -8:
		return new(int16)
	case t.SignBits < 0:
		return new(int8)
	case t.SignBits <= 8:
		return new(uint8)
	case t.SignBits <= 16:
		return new(uint16)
	case t.SignBits <= 32:
		return new(uint32)
	case t.SignBits <= 64:
		return new(uint64)
	}
	return &big.Int{}
}

func (t Int) ReflectType() reflect.Type {
	if t.SignBits == 0 {
		return intType
	}
	switch {
	case t.SignBits < -64:
		return bigIntType
	case t.SignBits < -32:
		return int64Type
	case t.SignBits < -16:
		return int32Type
	case t.SignBits < -8:
		return int16Type
	case t.SignBits < 0:
		return int8Type
	case t.SignBits <= 8:
		return uint8Type
	case t.SignBits <= 16:
		return uint16Type
	case t.SignBits <= 32:
		return uint32Type
	case t.SignBits <= 64:
		return uint64Type
	}
	return bigIntType
}

// String is an alphanumeric type
type String struct {
	// Length is the maximum length of the string that can be stored
	// without truncating.
	Length uint

	// Fixed indicates if the column is fixed width.  When false,
	// the string length is variable.
	Fixed bool
}

func (t String) New() interface{}          { return new(string) }
func (t String) ReflectType() reflect.Type { return stringType }

// Time is a time type
type Time struct {
	// Min is the minimum time the SQL data type can store
	Min time.Time

	// Max is the maximum time the data type can store
	Max time.Time

	// Prec is the precision of the data type
	Prec time.Duration
}

func (Time) New() interface{}          { return new(time.Time) }
func (Time) ReflectType() reflect.Type { return timeType }

// Nullable wraps another type to indicate that it is nullable
type Nullable [1]Type

// New returns a nullable version of the type it wraps.  If the wrapped type
// has a `*sql.Null____` counterpart, that type is returned.  Otherwise a
// double-pointer is returned.
func (t Nullable) New() interface{} {
	switch t[0].ReflectType().Kind() {
	case reflect.Bool:
		return &sql.NullBool{}
	case reflect.Float64, reflect.Float32:
		return &sql.NullFloat64{}
	case reflect.Int8, reflect.Int16:
		return &sql.NullInt16{}
	case reflect.Int32:
		return &sql.NullInt32{}
	case reflect.Int64:
		return &sql.NullInt64{}
	case reflect.Uint8:
		return &sql.NullByte{}
	case reflect.String:
		return &sql.NullString{}
	case reflect.Struct:
		if _, ok := t[0].(Time); ok {
			return &sql.NullTime{}
		}
	}
	return reflect.New(t.ReflectType()).Interface()
}

var (
	nullBoolType    = reflect.TypeOf((*sql.NullBool)(nil)).Elem()
	nullByteType    = reflect.TypeOf((*sql.NullByte)(nil)).Elem()
	nullFloat64Type = reflect.TypeOf((*sql.NullFloat64)(nil)).Elem()
	nullInt16Type   = reflect.TypeOf((*sql.NullInt16)(nil)).Elem()
	nullInt32Type   = reflect.TypeOf((*sql.NullInt32)(nil)).Elem()
	nullInt64Type   = reflect.TypeOf((*sql.NullInt64)(nil)).Elem()
	nullStringType  = reflect.TypeOf((*sql.NullString)(nil)).Elem()
	nullTimeType    = reflect.TypeOf((*sql.NullTime)(nil)).Elem()
)

func (t Nullable) ReflectType() reflect.Type {
	switch t[0].ReflectType().Kind() {
	case reflect.Bool:
		return nullBoolType
	case reflect.Float64, reflect.Float32:
		return nullFloat64Type
	case reflect.Int8, reflect.Int16:
		return nullInt16Type
	case reflect.Int32:
		return nullInt32Type
	case reflect.Int64:
		return nullInt64Type
	case reflect.Uint8:
		return nullByteType
	case reflect.String:
		return nullStringType
	case reflect.Struct:
		if _, ok := t[0].(Time); ok {
			return nullTimeType
		}
	}
	return reflect.PtrTo(t.ReflectType())
}

// Binary is an binary type
type Binary struct {
	// Length is the maximum length in bytes that can be stored
	// without truncating.
	Length uint

	// Fixed indicates if the column is fixed width.  When false,
	// the number of bytes is variable.
	Fixed bool
}

func (Binary) New() interface{}          { return new([]byte) }
func (Binary) ReflectType() reflect.Type { return bytesType }
