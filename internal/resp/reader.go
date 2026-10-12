package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrProtocol = errors.New("RESP protocol error")
)

type Reader struct {
	r              *bufio.Reader
	MaxBulkBytes   int
	MaxArrayLength int
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:              bufio.NewReader(r),
		MaxBulkBytes:   64 << 20, // 64 MiB safety cap for the teaching server.
		MaxArrayLength: 1024,
	}
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2:] != "\r\n" {
		return "", fmt.Errorf("%w: expected CRLF", ErrProtocol)
	}
	return line[:len(line)-2], nil
}

// ReadCommand reads the subset used by Redis command requests: an array of
// bulk strings. It also accepts inline commands to make telnet/nc debugging easy.
func (r *Reader) ReadCommand() ([]string, error) {
	b, err := r.r.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0] != '*' {
		return r.readInline()
	}
	return r.readArrayCommand()
}

func (r *Reader) readInline() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: empty inline command", ErrProtocol)
	}
	return fields, nil
}

func (r *Reader) readArrayCommand() ([]string, error) {
	header, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(header) < 2 || header[0] != '*' {
		return nil, fmt.Errorf("%w: expected array", ErrProtocol)
	}

	n, err := strconv.Atoi(header[1:])
	if err != nil || n <= 0 || n > r.MaxArrayLength {
		return nil, fmt.Errorf("%w: invalid array length", ErrProtocol)
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lengthLine, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(lengthLine) < 2 || lengthLine[0] != '$' {
			return nil, fmt.Errorf("%w: command element must be bulk string", ErrProtocol)
		}

		length, err := strconv.Atoi(lengthLine[1:])
		if err != nil || length < 0 || length > r.MaxBulkBytes {
			return nil, fmt.Errorf("%w: invalid bulk length", ErrProtocol)
		}

		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		if buf[length] != '\r' || buf[length+1] != '\n' {
			return nil, fmt.Errorf("%w: malformed bulk string", ErrProtocol)
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}
