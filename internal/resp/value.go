package resp

type Type uint8

const (
	SimpleStringType Type = iota + 1
	ErrorType
	IntegerType
	BulkStringType
	NullBulkStringType
	ArrayType
	NullArrayType
	NullType
	BooleanType
	DoubleType
	BigNumberType
	VerbatimStringType
	MapType
	SetType
	PushType
)

type MapEntry struct {
	Key   Value
	Value Value
}

type Value struct {
	Type   Type
	Str    string
	Int    int64
	Float  float64
	Bool   bool
	Format string
	Array  []Value
	Map    []MapEntry
}

func SimpleString(s string) Value { return Value{Type: SimpleStringType, Str: s} }
func Error(s string) Value        { return Value{Type: ErrorType, Str: s} }
func Integer(n int64) Value       { return Value{Type: IntegerType, Int: n} }
func BulkString(s string) Value   { return Value{Type: BulkStringType, Str: s} }
func NullBulkString() Value       { return Value{Type: NullBulkStringType} }
func Array(values []Value) Value  { return Value{Type: ArrayType, Array: values} }
func NullArray() Value            { return Value{Type: NullArrayType} }

func Null() Value                   { return Value{Type: NullType} }
func Boolean(v bool) Value          { return Value{Type: BooleanType, Bool: v} }
func Double(v float64) Value        { return Value{Type: DoubleType, Float: v} }
func BigNumber(v string) Value      { return Value{Type: BigNumberType, Str: v} }
func Verbatim(format, text string) Value {
	return Value{Type: VerbatimStringType, Format: format, Str: text}
}
func MapValue(entries []MapEntry) Value { return Value{Type: MapType, Map: entries} }
func Set(values []Value) Value          { return Value{Type: SetType, Array: values} }
func Push(values []Value) Value         { return Value{Type: PushType, Array: values} }

func (v Value) IsError() bool { return v.Type == ErrorType }
