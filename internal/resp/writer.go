package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

func (w *Writer) WriteValue(v Value) error {
	return w.WriteValueProtocol(v, Protocol2)
}

func (w *Writer) WriteValueProtocol(v Value, protocol Protocol) error {
	if err := writeValueProtocol(w.w, v, protocol); err != nil {
		return err
	}
	return w.w.Flush()
}

func writeValueProtocol(w io.Writer, v Value, protocol Protocol) error {
	if protocol == Protocol3 {
		return writeRESP3(w, v)
	}
	return writeRESP2(w, v)
}

func writeRESP2(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleStringType:
		_, err := fmt.Fprintf(w, "+%s\r\n", sanitizeLine(v.Str))
		return err
	case ErrorType:
		_, err := fmt.Fprintf(w, "-%s\r\n", sanitizeLine(v.Str))
		return err
	case IntegerType:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Int)
		return err
	case BulkStringType:
		return writeBlob(w, '$', v.Str)
	case NullBulkStringType, NullType:
		_, err := io.WriteString(w, "$-1\r\n")
		return err
	case ArrayType:
		return writeArrayLike(w, '*', v.Array, Protocol2)
	case NullArrayType:
		_, err := io.WriteString(w, "*-1\r\n")
		return err
	case BooleanType:
		if v.Bool {
			_, err := io.WriteString(w, ":1\r\n")
			return err
		}
		_, err := io.WriteString(w, ":0\r\n")
		return err
	case DoubleType:
		return writeBlob(w, '$', formatDouble(v.Float))
	case BigNumberType:
		return writeBlob(w, '$', v.Str)
	case VerbatimStringType:
		return writeBlob(w, '$', v.Str)
	case MapType:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Map)*2); err != nil {
			return err
		}
		for _, entry := range v.Map {
			if err := writeRESP2(w, entry.Key); err != nil {
				return err
			}
			if err := writeRESP2(w, entry.Value); err != nil {
				return err
			}
		}
		return nil
	case SetType, PushType:
		return writeArrayLike(w, '*', v.Array, Protocol2)
	default:
		return fmt.Errorf("unsupported RESP value type %d", v.Type)
	}
}

func writeRESP3(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleStringType:
		_, err := fmt.Fprintf(w, "+%s\r\n", sanitizeLine(v.Str))
		return err
	case ErrorType:
		_, err := fmt.Fprintf(w, "-%s\r\n", sanitizeLine(v.Str))
		return err
	case IntegerType:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Int)
		return err
	case BulkStringType:
		return writeBlob(w, '$', v.Str)
	case NullBulkStringType, NullArrayType, NullType:
		_, err := io.WriteString(w, "_\r\n")
		return err
	case ArrayType:
		return writeArrayLike(w, '*', v.Array, Protocol3)
	case BooleanType:
		if v.Bool {
			_, err := io.WriteString(w, "#t\r\n")
			return err
		}
		_, err := io.WriteString(w, "#f\r\n")
		return err
	case DoubleType:
		_, err := fmt.Fprintf(w, ",%s\r\n", formatDouble(v.Float))
		return err
	case BigNumberType:
		_, err := fmt.Fprintf(w, "(%s\r\n", sanitizeLine(v.Str))
		return err
	case VerbatimStringType:
		format := v.Format
		if len(format) != 3 {
			format = "txt"
		}
		return writeBlob(w, '=', format+":"+v.Str)
	case MapType:
		if _, err := fmt.Fprintf(w, "%%%d\r\n", len(v.Map)); err != nil {
			return err
		}
		for _, entry := range v.Map {
			if err := writeRESP3(w, entry.Key); err != nil {
				return err
			}
			if err := writeRESP3(w, entry.Value); err != nil {
				return err
			}
		}
		return nil
	case SetType:
		return writeArrayLike(w, '~', v.Array, Protocol3)
	case PushType:
		return writeArrayLike(w, '>', v.Array, Protocol3)
	default:
		return fmt.Errorf("unsupported RESP value type %d", v.Type)
	}
}

func writeBlob(w io.Writer, prefix byte, payload string) error {
	if _, err := fmt.Fprintf(w, "%c%d\r\n", prefix, len(payload)); err != nil {
		return err
	}
	_, err := io.WriteString(w, payload+"\r\n")
	return err
}

func writeArrayLike(w io.Writer, prefix byte, values []Value, protocol Protocol) error {
	if _, err := fmt.Fprintf(w, "%c%d\r\n", prefix, len(values)); err != nil {
		return err
	}
	for _, child := range values {
		if err := writeValueProtocol(w, child, protocol); err != nil {
			return err
		}
	}
	return nil
}

func formatDouble(v float64) string {
	switch {
	case math.IsNaN(v):
		return "nan"
	case math.IsInf(v, 1):
		return "inf"
	case math.IsInf(v, -1):
		return "-inf"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

func sanitizeLine(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
}

// EncodeValue serializes using the historical RESP2 compatibility mode.
func EncodeValue(v Value) ([]byte, error) {
	return EncodeValueProtocol(v, Protocol2)
}

func EncodeValueProtocol(v Value, protocol Protocol) ([]byte, error) {
	var b bytes.Buffer
	if err := writeValueProtocol(&b, v, protocol); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// EncodeCommand serializes a command as an array of blob strings. This request
// representation is valid for RESP2 and RESP3 connections, and AOF continues to
// use it so persistence never depends on negotiated client response mode.
func EncodeCommand(args []string) []byte {
	var b strings.Builder
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, arg := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(arg)))
		b.WriteString("\r\n")
		b.WriteString(arg)
		b.WriteString("\r\n")
	}
	return []byte(b.String())
}
