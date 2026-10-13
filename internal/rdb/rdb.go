package rdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/1998x-stack/mini-redis/internal/persistence"
	"github.com/1998x-stack/mini-redis/internal/store"
)

const (
	Magic       = "MRDB"
	Version     = uint16(1)
	headerSize  = 4 + 2 + 8
	checksumLen = 4
)

var (
	ErrMagic      = errors.New("invalid RDB magic")
	ErrVersion    = errors.New("unsupported RDB version")
	ErrTruncated  = errors.New("truncated RDB file")
	ErrChecksum   = errors.New("RDB checksum mismatch")
	ErrPayloadLen = errors.New("invalid RDB payload length")
)

type payload struct {
	Entries []store.SnapshotEntry `json:"entries"`
}

// Encode writes only logical state. Physical Object.Encoding values and private
// Go payloads are intentionally absent from the file format.
func Encode(entries []store.SnapshotEntry) ([]byte, error) {
	ordered := append([]store.SnapshotEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	body, err := json.Marshal(payload{Entries: ordered})
	if err != nil {
		return nil, fmt.Errorf("encode RDB payload: %w", err)
	}

	buf := bytes.NewBuffer(make([]byte, 0, headerSize+len(body)+checksumLen))
	buf.WriteString(Magic)
	_ = binary.Write(buf, binary.BigEndian, Version)
	_ = binary.Write(buf, binary.BigEndian, uint64(len(body)))
	buf.Write(body)
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	_ = binary.Write(buf, binary.BigEndian, checksum)
	return buf.Bytes(), nil
}

func Decode(data []byte) ([]store.SnapshotEntry, error) {
	if len(data) < headerSize+checksumLen {
		return nil, ErrTruncated
	}
	if string(data[:4]) != Magic {
		return nil, ErrMagic
	}
	version := binary.BigEndian.Uint16(data[4:6])
	if version != Version {
		return nil, fmt.Errorf("%w: got %d want %d", ErrVersion, version, Version)
	}
	payloadLen := binary.BigEndian.Uint64(data[6:14])
	if payloadLen > uint64(len(data)-headerSize-checksumLen) {
		return nil, ErrTruncated
	}
	expectedLen := headerSize + int(payloadLen) + checksumLen
	if expectedLen != len(data) {
		return nil, ErrPayloadLen
	}
	want := binary.BigEndian.Uint32(data[len(data)-checksumLen:])
	got := crc32.ChecksumIEEE(data[:len(data)-checksumLen])
	if got != want {
		return nil, ErrChecksum
	}

	var p payload
	if err := json.Unmarshal(data[headerSize:len(data)-checksumLen], &p); err != nil {
		return nil, fmt.Errorf("decode RDB payload: %w", err)
	}
	return p.Entries, nil
}

func SaveFile(path string, entries []store.SnapshotEntry) error {
	data, err := Encode(entries)
	if err != nil {
		return err
	}
	return persistence.WriteFileAtomic(path, data, 0o644)
}

func LoadFile(path string) ([]store.SnapshotEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Decode(data)
}
