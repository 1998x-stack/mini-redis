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
)

type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
}

func SimpleString(s string) Value { return Value{Type: SimpleStringType, Str: s} }
func Error(s string) Value        { return Value{Type: ErrorType, Str: s} }
func Integer(n int64) Value       { return Value{Type: IntegerType, Int: n} }
func BulkString(s string) Value   { return Value{Type: BulkStringType, Str: s} }
func NullBulkString() Value       { return Value{Type: NullBulkStringType} }
func Array(values []Value) Value  { return Value{Type: ArrayType, Array: values} }
func NullArray() Value            { return Value{Type: NullArrayType} }

func (v Value) IsError() bool { return v.Type == ErrorType }
