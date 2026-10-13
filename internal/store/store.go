package store

import (
	"errors"
	"strconv"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrWrongType  = errors.New("operation against a key holding the wrong kind of value")
)

// ObjectType is the logical Redis data type exposed by command semantics.
type ObjectType uint8

const (
	ObjectString ObjectType = iota + 1
	ObjectList
	ObjectHash
	ObjectSet
	ObjectZSet
)

func (t ObjectType) String() string {
	switch t {
	case ObjectString:
		return "string"
	case ObjectList:
		return "list"
	case ObjectHash:
		return "hash"
	case ObjectSet:
		return "set"
	case ObjectZSet:
		return "zset"
	default:
		return "unknown"
	}
}

// ObjectEncoding is the physical representation behind one logical type.
// v0.4 intentionally reuses names familiar from Redis while implementing small
// educational equivalents rather than byte-for-byte production structures.
type ObjectEncoding uint8

const (
	EncodingRaw ObjectEncoding = iota + 1
	EncodingInt
	EncodingListPack
	EncodingQuickList
	EncodingHashTable
	EncodingIntSet
	EncodingSkipList
)

func (e ObjectEncoding) String() string {
	switch e {
	case EncodingRaw:
		return "raw"
	case EncodingInt:
		return "int"
	case EncodingListPack:
		return "listpack"
	case EncodingQuickList:
		return "quicklist"
	case EncodingHashTable:
		return "hashtable"
	case EncodingIntSet:
		return "intset"
	case EncodingSkipList:
		return "skiplist"
	default:
		return "unknown"
	}
}

// Object separates the stable logical type from its replaceable physical
// representation. Payload is package-owned so command handlers cannot couple
// themselves to a concrete encoding.
type Object struct {
	Type     ObjectType
	Encoding ObjectEncoding
	payload  any
}

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

type FieldValue struct {
	Field string
	Value string
}

type ScoredMember struct {
	Member string
	Score  float64
}

type TTLResult struct {
	Exists    bool
	HasExpiry bool
	Remaining time.Duration
}

// Store is the command-facing data-engine boundary. v0.4 adds collection
// semantics here so atomicity and TTL preservation stay inside the engine
// rather than leaking read-modify-write sequences into command handlers.
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
	ExpireDue(limit int) int
	Len() int

	ListPush(key string, left bool, values []string) (int, error)
	ListPop(key string, left bool) (string, bool, error)
	ListLen(key string) (int, error)
	ListRange(key string, start, stop int) ([]string, error)

	HashSet(key string, pairs []FieldValue) (int, error)
	HashGet(key, field string) (string, bool, error)
	HashDelete(key string, fields ...string) (int, error)
	HashLen(key string) (int, error)
	HashGetAll(key string) ([]FieldValue, error)

	SetAdd(key string, members ...string) (int, error)
	SetRemove(key string, members ...string) (int, error)
	SetIsMember(key, member string) (bool, error)
	SetMembers(key string) ([]string, error)
	SetCard(key string) (int, error)

	ZAdd(key string, pairs []ScoredMember) (int, error)
	ZScore(key, member string) (float64, bool, error)
	ZRank(key, member string) (int, bool, error)
	ZRange(key string, start, stop int) ([]ScoredMember, error)
	ZRemove(key string, members ...string) (int, error)
}

// RehashStepper is an optional maintenance capability used by server timers.
type RehashStepper interface {
	RehashStep(workUnits int) int
}
