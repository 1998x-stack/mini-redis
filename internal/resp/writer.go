package resp

import (
	"bufio"
	"fmt"
	"io"
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
	if err := writeValue(w.w, v); err != nil {
		return err
	}
	return w.w.Flush()
}

func writeValue(w io.Writer, v Value) error {
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
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(v.Str)); err != nil {
			return err
		}
		_, err := io.WriteString(w, v.Str+"\r\n")
		return err
	case NullBulkStringType:
		_, err := io.WriteString(w, "$-1\r\n")
		return err
	case ArrayType:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Array)); err != nil {
			return err
		}
		for _, child := range v.Array {
			if err := writeValue(w, child); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported RESP value type %d", v.Type)
	}
}

func sanitizeLine(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
}

// EncodeCommand serializes a command as RESP2. AOF uses exactly this format,
// so the normal parser can replay the log without a second grammar.
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
