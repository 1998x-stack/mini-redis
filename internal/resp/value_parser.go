package resp

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const maxValueDepth = 64

// ParseValue parses one RESP2/RESP3 value from data without consuming an
// incomplete frame. complete=false means the prefix is valid so far and more
// bytes are required. This parser is used by tests/demos and future async
// clients; command requests continue to use ParseCommand.
func ParseValue(data []byte) (Value, int, bool, error) {
	value, next, complete, err := parseValueAt(data, 0, 0)
	if err != nil || !complete {
		return Value{}, 0, complete, err
	}
	return value, next, true, nil
}

func parseValueAt(data []byte, offset, depth int) (Value, int, bool, error) {
	if depth > maxValueDepth {
		return Value{}, 0, false, fmt.Errorf("%w: nesting depth exceeded", ErrProtocol)
	}
	if offset >= len(data) {
		return Value{}, 0, false, nil
	}
	prefix := data[offset]
	offset++

	readLine := func() (string, int, bool) {
		idx := bytes.Index(data[offset:], []byte("\r\n"))
		if idx < 0 {
			return "", 0, false
		}
		return string(data[offset : offset+idx]), offset + idx + 2, true
	}

	switch prefix {
	case '+', '-', ':', ',', '(', '#', '_':
		line, next, ok := readLine()
		if !ok {
			return Value{}, 0, false, nil
		}
		switch prefix {
		case '+':
			return SimpleString(line), next, true, nil
		case '-':
			return Error(line), next, true, nil
		case ':':
			n, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return Value{}, 0, false, fmt.Errorf("%w: invalid integer", ErrProtocol)
			}
			return Integer(n), next, true, nil
		case ',':
			var f float64
			var err error
			switch strings.ToLower(line) {
			case "inf", "+inf":
				f = math.Inf(1)
			case "-inf":
				f = math.Inf(-1)
			case "nan":
				f = math.NaN()
			default:
				f, err = strconv.ParseFloat(line, 64)
			}
			if err != nil {
				return Value{}, 0, false, fmt.Errorf("%w: invalid double", ErrProtocol)
			}
			return Double(f), next, true, nil
		case '(':
			if line == "" {
				return Value{}, 0, false, fmt.Errorf("%w: empty big number", ErrProtocol)
			}
			return BigNumber(line), next, true, nil
		case '#':
			if line == "t" {
				return Boolean(true), next, true, nil
			}
			if line == "f" {
				return Boolean(false), next, true, nil
			}
			return Value{}, 0, false, fmt.Errorf("%w: invalid boolean", ErrProtocol)
		case '_':
			if line != "" {
				return Value{}, 0, false, fmt.Errorf("%w: invalid null", ErrProtocol)
			}
			return Null(), next, true, nil
		}
	case '$', '=':
		line, next, ok := readLine()
		if !ok {
			return Value{}, 0, false, nil
		}
		length, err := strconv.Atoi(line)
		if err != nil || length < -1 {
			return Value{}, 0, false, fmt.Errorf("%w: invalid blob length", ErrProtocol)
		}
		if length == -1 {
			if prefix == '=' {
				return Value{}, 0, false, fmt.Errorf("%w: null verbatim string", ErrProtocol)
			}
			return NullBulkString(), next, true, nil
		}
		if len(data)-next < length+2 {
			return Value{}, 0, false, nil
		}
		if data[next+length] != '\r' || data[next+length+1] != '\n' {
			return Value{}, 0, false, fmt.Errorf("%w: malformed blob string", ErrProtocol)
		}
		payload := string(data[next : next+length])
		end := next + length + 2
		if prefix == '$' {
			return BulkString(payload), end, true, nil
		}
		if len(payload) < 4 || payload[3] != ':' {
			return Value{}, 0, false, fmt.Errorf("%w: invalid verbatim string", ErrProtocol)
		}
		return Verbatim(payload[:3], payload[4:]), end, true, nil
	case '*', '~', '>':
		line, next, ok := readLine()
		if !ok {
			return Value{}, 0, false, nil
		}
		count, err := strconv.Atoi(line)
		if err != nil || count < -1 {
			return Value{}, 0, false, fmt.Errorf("%w: invalid container length", ErrProtocol)
		}
		if count == -1 {
			if prefix != '*' {
				return Value{}, 0, false, fmt.Errorf("%w: null non-array container", ErrProtocol)
			}
			return NullArray(), next, true, nil
		}
		values := make([]Value, 0, count)
		cursor := next
		for i := 0; i < count; i++ {
			child, childNext, complete, err := parseValueAt(data, cursor, depth+1)
			if err != nil || !complete {
				return Value{}, 0, complete, err
			}
			values = append(values, child)
			cursor = childNext
		}
		switch prefix {
		case '*':
			return Array(values), cursor, true, nil
		case '~':
			return Set(values), cursor, true, nil
		default:
			return Push(values), cursor, true, nil
		}
	case '%':
		line, next, ok := readLine()
		if !ok {
			return Value{}, 0, false, nil
		}
		count, err := strconv.Atoi(line)
		if err != nil || count < 0 {
			return Value{}, 0, false, fmt.Errorf("%w: invalid map length", ErrProtocol)
		}
		entries := make([]MapEntry, 0, count)
		cursor := next
		for i := 0; i < count; i++ {
			key, keyNext, complete, err := parseValueAt(data, cursor, depth+1)
			if err != nil || !complete {
				return Value{}, 0, complete, err
			}
			value, valueNext, complete, err := parseValueAt(data, keyNext, depth+1)
			if err != nil || !complete {
				return Value{}, 0, complete, err
			}
			entries = append(entries, MapEntry{Key: key, Value: value})
			cursor = valueNext
		}
		return MapValue(entries), cursor, true, nil
	default:
		return Value{}, 0, false, fmt.Errorf("%w: unsupported value prefix %q", ErrProtocol, prefix)
	}
	return Value{}, 0, false, fmt.Errorf("%w: invalid value", ErrProtocol)
}
