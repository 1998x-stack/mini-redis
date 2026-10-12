package store

import (
	"errors"
	"strconv"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
)

// ObjectType is the logical Redis value kind. v0.3 still implements only
// strings; v0.4 will add collection types without changing the distinction
// between logical type and physical encoding introduced here.
type ObjectType uint8

const (
	ObjectString ObjectType = iota + 1
)

func (t ObjectType) String() string {
	switch t {
	case ObjectString:
		return "string"
	default:
		return "unknown"
	}
}

// ObjectEncoding describes how the logical object is physically represented.
// A Redis string can be a raw byte/string payload or an integer payload while
// preserving the same GET/SET semantics.
type ObjectEncoding uint8

const (
	EncodingRaw ObjectEncoding = iota + 1
	EncodingInt
)

func (e ObjectEncoding) String() string {
	switch e {
	case EncodingRaw:
		return "raw"
	case EncodingInt:
		return "int"
	default:
		return "unknown"
	}
}

// Object is deliberately small and explicit. Payload is encoding-specific;
// constructors and accessors keep command handlers from depending on its
// concrete Go representation.
type Object struct {
	Type     ObjectType
	Encoding ObjectEncoding
	payload  any
}

// NewString chooses integer encoding only when converting to int64 and back is
// byte-for-byte canonical. That preserves values such as "001" or "+1" as raw
// strings instead of silently changing what GET returns.
func NewString(value string) Object {
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && strconv.FormatInt(n, 10) == value {
		return Object{Type: ObjectString, Encoding: EncodingInt, payload: n}
	}
	return Object{Type: ObjectString, Encoding: EncodingRaw, payload: value}
}

func (o Object) StringValue() (string, bool) {
	if o.Type != ObjectString {
		return "", false
	}
	switch o.Encoding {
	case EncodingRaw:
		v, ok := o.payload.(string)
		return v, ok
	case EncodingInt:
		v, ok := o.payload.(int64)
		if !ok {
			return "", false
		}
		return strconv.FormatInt(v, 10), true
	default:
		return "", false
	}
}

func (o Object) Int64() (int64, bool) {
	if o.Type != ObjectString || o.Encoding != EncodingInt {
		return 0, false
	}
	v, ok := o.payload.(int64)
	return v, ok
}

// TTLResult mirrors the three states Redis users care about:
//   - key missing
//   - key exists but has no expiration
//   - key exists and has a remaining TTL
type TTLResult struct {
	Exists    bool
	HasExpiry bool
	Remaining time.Duration
}

// Store is the stable data-engine boundary. v0.3 replaces the implementation's
// Go maps with an explicit incremental-rehash dictionary without changing this
// command-facing contract.
type Store interface {
	Get(key string) (Object, bool)
	Set(key string, value Object, ttl *time.Duration)
	Delete(keys ...string) int
	Exists(keys ...string) int
	IncrBy(key string, delta int64) (int64, error)
	Expire(key string, ttl time.Duration) bool
	ExpireAt(key string, deadline time.Time) bool
	TTL(key string) TTLResult
	Keys(pattern string) ([]string, error)
	Flush()

	// ExpireDue performs active expiration. limit <= 0 means full scan.
	// It returns the number of keys physically removed.
	ExpireDue(limit int) int
	Len() int
}

// RehashStepper is an optional maintenance capability. Server runtimes can
// advance bounded dictionary work without coupling command semantics to the
// concrete memory engine.
type RehashStepper interface {
	RehashStep(workUnits int) int
}
