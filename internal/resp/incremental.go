package resp

import (
	"bytes"
	"errors"
	"io"
)

// ParseCommand parses at most one RESP command from data without consuming a
// partial frame. It is designed for event-loop clients that own their input
// buffers across multiple nonblocking reads.
//
// complete=false means the prefix is valid so far but more bytes are needed.
// A protocol error is returned only when enough bytes exist to prove the frame
// is malformed.
func ParseCommand(data []byte) (argv []string, consumed int, complete bool, err error) {
	if len(data) == 0 {
		return nil, 0, false, nil
	}

	src := bytes.NewReader(data)
	reader := NewReader(src)
	argv, err = reader.ReadCommand()
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}

	// bufio.Reader may read beyond the first command. Bytes still buffered by
	// it are logically unconsumed and must remain in the client's input buffer.
	consumed = len(data) - src.Len() - reader.r.Buffered()
	if consumed <= 0 || consumed > len(data) {
		return nil, 0, false, errors.New("RESP parser made invalid progress")
	}
	return argv, consumed, true, nil
}
