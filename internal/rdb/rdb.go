package rdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"sort"
	"strconv"

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
	Entries []wireEntry `json:"entries"`
}

type wireEntry struct {
	Key               string             `json:"key"`
	Type              store.ObjectType   `json:"type"`
	ExpireAtUnixMilli int64              `json:"expire_at_ms,omitempty"`
	StringValue       string             `json:"string,omitempty"`
	ListValues        []string           `json:"list,omitempty"`
	HashValues        []store.FieldValue `json:"hash,omitempty"`
	SetValues         []string           `json:"set,omitempty"`
	ZSetValues        []wireScoredMember `json:"zset,omitempty"`
}

type wireScoredMember struct {
	Member string `json:"member"`
	Score  string `json:"score"`
}

func toWire(entries []store.SnapshotEntry) []wireEntry {
	ordered := append([]store.SnapshotEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	out := make([]wireEntry, 0, len(ordered))
	for _, entry := range ordered {
		w := wireEntry{
			Key:               entry.Key,
			Type:              entry.Type,
			ExpireAtUnixMilli: entry.ExpireAtUnixMilli,
			StringValue:       entry.StringValue,
			ListValues:        append([]string(nil), entry.ListValues...),
			HashValues:        append([]store.FieldValue(nil), entry.HashValues...),
			SetValues:         append([]string(nil), entry.SetValues...),
		}
		sort.Slice(w.HashValues, func(i, j int) bool { return w.HashValues[i].Field < w.HashValues[j].Field })
		sort.Strings(w.SetValues)
		zset := append([]store.ScoredMember(nil), entry.ZSetValues...)
		sort.Slice(zset, func(i, j int) bool {
			return zset[i].Score < zset[j].Score || (zset[i].Score == zset[j].Score && zset[i].Member < zset[j].Member)
		})
		for _, pair := range zset {
			w.ZSetValues = append(w.ZSetValues, wireScoredMember{
				Member: pair.Member,
				Score:  strconv.FormatFloat(pair.Score, 'g', -1, 64),
			})
		}
		out = append(out, w)
	}
	return out
}

func fromWire(entries []wireEntry) ([]store.SnapshotEntry, error) {
	out := make([]store.SnapshotEntry, 0, len(entries))
	for _, w := range entries {
		entry := store.SnapshotEntry{
			Key:               w.Key,
			Type:              w.Type,
			ExpireAtUnixMilli: w.ExpireAtUnixMilli,
			StringValue:       w.StringValue,
			ListValues:        append([]string(nil), w.ListValues...),
			HashValues:        append([]store.FieldValue(nil), w.HashValues...),
			SetValues:         append([]string(nil), w.SetValues...),
		}
		for _, pair := range w.ZSetValues {
			score, err := strconv.ParseFloat(pair.Score, 64)
			if err != nil || math.IsNaN(score) {
				return nil, fmt.Errorf("decode RDB zset score %q: invalid float", pair.Score)
			}
			entry.ZSetValues = append(entry.ZSetValues, store.ScoredMember{Member: pair.Member, Score: score})
		}
		out = append(out, entry)
	}
	return out, nil
}

// Encode writes only logical state. Physical Object.Encoding values and private
// Go payloads are intentionally absent from the file format. Scores use a
// canonical string wire representation so +/-Inf remain round-trippable even
// though JSON itself cannot encode infinite numeric literals.
func Encode(entries []store.SnapshotEntry) ([]byte, error) {
	body, err := json.Marshal(payload{Entries: toWire(entries)})
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
	entries, err := fromWire(p.Entries)
	if err != nil {
		return nil, err
	}
	return entries, nil
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
