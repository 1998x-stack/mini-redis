// Package dict implements the educational Redis-like hash table introduced in
// mini-redis v0.3. It is intentionally not concurrency-safe: the Store owns
// synchronization, which keeps hashing/rehashing mechanics independent from
// concurrency policy.
package dict

const (
	defaultInitialSize    = 8
	growLoadNumerator     = 1 // grow when used >= size
	growLoadDenominator   = 1
	shrinkLoadNumerator   = 1 // shrink when used/size < 1/10
	shrinkLoadDenominator = 10
)

// Hasher maps a string key to a stable 64-bit hash. Supplying it explicitly is
// useful for collision tests and keeps the hash algorithm separate from table
// mechanics.
type Hasher func(string) uint64

// FNV1a is a compact deterministic reference hash. Production Redis uses a
// different hash strategy; v0.3 cares about the dictionary boundary, collision
// handling and resize behavior rather than matching Redis' exact hash bits.
func FNV1a(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

type entry[V any] struct {
	key   string
	value V
	next  *entry[V]
}

type table[V any] struct {
	buckets []*entry[V]
	used    int
}

func newTable[V any](size int) table[V] {
	return table[V]{buckets: make([]*entry[V], normalizeSize(size))}
}

func (t *table[V]) size() int { return len(t.buckets) }

// Dict uses separate chaining and two tables while resizing. During rehash,
// new keys go to ht[1], while lookups search both tables.
//
// Rehash work is deliberately finer-grained than a whole bucket: one work unit
// moves at most one entry, or advances across one empty bucket. This gives the
// event loop a strict, inspectable upper bound per maintenance call even when a
// collision chain is long.
type Dict[V any] struct {
	ht          [2]table[V]
	rehashIndex int // -1 when not rehashing
	hasher      Hasher
	minSize     int
}

// Stats exposes internal state for tests, teaching tools and benchmarks. It is
// not a Redis command API.
type Stats struct {
	Table0Size  int
	Table0Used  int
	Table1Size  int
	Table1Used  int
	Rehashing   bool
	RehashIndex int
}

func New[V any](hasher Hasher) *Dict[V] {
	return NewWithSize[V](defaultInitialSize, hasher)
}

func NewWithSize[V any](initialSize int, hasher Hasher) *Dict[V] {
	if hasher == nil {
		hasher = FNV1a
	}
	initialSize = normalizeSize(initialSize)
	return &Dict[V]{
		ht:          [2]table[V]{newTable[V](initialSize), {}},
		rehashIndex: -1,
		hasher:      hasher,
		minSize:     initialSize,
	}
}

func (d *Dict[V]) Len() int { return d.ht[0].used + d.ht[1].used }

func (d *Dict[V]) IsRehashing() bool { return d.rehashIndex >= 0 }

func (d *Dict[V]) Stats() Stats {
	return Stats{
		Table0Size:  d.ht[0].size(),
		Table0Used:  d.ht[0].used,
		Table1Size:  d.ht[1].size(),
		Table1Used:  d.ht[1].used,
		Rehashing:   d.IsRehashing(),
		RehashIndex: d.rehashIndex,
	}
}

// Get performs one opportunistic rehash work unit before lookup, mirroring the
// idea that normal traffic helps resize progress.
func (d *Dict[V]) Get(key string) (V, bool) {
	d.RehashStep(1)
	if e := d.findEntry(0, key); e != nil {
		return e.value, true
	}
	if d.IsRehashing() {
		if e := d.findEntry(1, key); e != nil {
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

// Peek is a non-progressing lookup for iteration/testing paths that must not
// mutate rehash state.
func (d *Dict[V]) Peek(key string) (V, bool) {
	if e := d.findEntry(0, key); e != nil {
		return e.value, true
	}
	if d.IsRehashing() {
		if e := d.findEntry(1, key); e != nil {
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

// Set inserts or updates a key. Updates modify the table that currently owns
// the logical key; a key is never intentionally duplicated across ht[0]/ht[1].
// The return value is true when a new key was inserted.
func (d *Dict[V]) Set(key string, value V) bool {
	d.RehashStep(1)

	if e := d.findEntry(0, key); e != nil {
		e.value = value
		return false
	}
	if d.IsRehashing() {
		if e := d.findEntry(1, key); e != nil {
			e.value = value
			return false
		}
	}

	if !d.IsRehashing() && d.shouldGrow() {
		d.startResize(d.ht[0].size() * 2)
	}
	tableIndex := 0
	if d.IsRehashing() {
		tableIndex = 1
	}
	d.insertNew(tableIndex, key, value)
	return true
}

// Delete removes a logical key regardless of which table currently owns it.
func (d *Dict[V]) Delete(key string) bool {
	d.RehashStep(1)
	if d.deleteFrom(0, key) {
		d.maybeStartShrink()
		return true
	}
	if d.IsRehashing() && d.deleteFrom(1, key) {
		return true
	}
	return false
}

// RehashStep performs at most workUnits bounded units. One unit either moves a
// single entry from ht[0] to ht[1], or advances past one empty bucket. It
// returns the number of work units actually consumed.
func (d *Dict[V]) RehashStep(workUnits int) int {
	if workUnits <= 0 || !d.IsRehashing() {
		return 0
	}

	used := 0
	for used < workUnits && d.IsRehashing() {
		if d.ht[0].used == 0 || d.rehashIndex >= d.ht[0].size() {
			d.finishRehash()
			break
		}

		bucket := &d.ht[0].buckets[d.rehashIndex]
		if *bucket == nil {
			d.rehashIndex++
			used++
			continue
		}

		// Move one chain head only; leave the rest for future work units.
		e := *bucket
		*bucket = e.next
		d.ht[0].used--
		e.next = nil
		d.insertExisting(1, e)
		used++

		if *bucket == nil {
			d.rehashIndex++
		}
		if d.ht[0].used == 0 {
			d.finishRehash()
		}
	}
	return used
}

// Range visits each logical key exactly once as long as the callback does not
// mutate this Dict. Mutating the dictionary from inside fn is unsupported; this
// explicit policy avoids pretending to offer a stable concurrent iterator.
func (d *Dict[V]) Range(fn func(key string, value V) bool) {
	for tableIndex := 0; tableIndex < 2; tableIndex++ {
		if tableIndex == 1 && !d.IsRehashing() {
			break
		}
		for _, head := range d.ht[tableIndex].buckets {
			for e := head; e != nil; e = e.next {
				if !fn(e.key, e.value) {
					return
				}
			}
		}
	}
}

// Keys returns up to limit keys without advancing rehash. limit <= 0 means all
// keys. The order is intentionally unspecified.
func (d *Dict[V]) Keys(limit int) []string {
	capacity := d.Len()
	if limit > 0 && capacity > limit {
		capacity = limit
	}
	out := make([]string, 0, capacity)
	d.Range(func(key string, _ V) bool {
		out = append(out, key)
		return limit <= 0 || len(out) < limit
	})
	return out
}

func (d *Dict[V]) findEntry(tableIndex int, key string) *entry[V] {
	t := &d.ht[tableIndex]
	if t.size() == 0 {
		return nil
	}
	idx := d.indexFor(key, t.size())
	for e := t.buckets[idx]; e != nil; e = e.next {
		if e.key == key {
			return e
		}
	}
	return nil
}

func (d *Dict[V]) insertNew(tableIndex int, key string, value V) {
	e := &entry[V]{key: key, value: value}
	d.insertExisting(tableIndex, e)
}

func (d *Dict[V]) insertExisting(tableIndex int, e *entry[V]) {
	t := &d.ht[tableIndex]
	idx := d.indexFor(e.key, t.size())
	e.next = t.buckets[idx]
	t.buckets[idx] = e
	t.used++
}

func (d *Dict[V]) deleteFrom(tableIndex int, key string) bool {
	t := &d.ht[tableIndex]
	if t.size() == 0 {
		return false
	}
	idx := d.indexFor(key, t.size())
	var prev *entry[V]
	for e := t.buckets[idx]; e != nil; e = e.next {
		if e.key != key {
			prev = e
			continue
		}
		if prev == nil {
			t.buckets[idx] = e.next
		} else {
			prev.next = e.next
		}
		t.used--
		return true
	}
	return false
}

func (d *Dict[V]) shouldGrow() bool {
	size := d.ht[0].size()
	if size == 0 {
		return true
	}
	return d.ht[0].used*growLoadDenominator >= size*growLoadNumerator
}

func (d *Dict[V]) maybeStartShrink() {
	if d.IsRehashing() || d.ht[0].size() <= d.minSize {
		return
	}
	if d.ht[0].used*shrinkLoadDenominator >= d.ht[0].size()*shrinkLoadNumerator {
		return
	}
	target := normalizeSize(maxInt(d.minSize, d.ht[0].used*2))
	if target < d.ht[0].size() {
		d.startResize(target)
	}
}

func (d *Dict[V]) startResize(size int) bool {
	if d.IsRehashing() {
		return false
	}
	size = normalizeSize(maxInt(d.minSize, size))
	if size == d.ht[0].size() {
		return false
	}
	d.ht[1] = newTable[V](size)
	d.rehashIndex = 0
	return true
}

func (d *Dict[V]) finishRehash() {
	if !d.IsRehashing() {
		return
	}
	d.ht[0] = d.ht[1]
	d.ht[1] = table[V]{}
	d.rehashIndex = -1
}

func (d *Dict[V]) indexFor(key string, size int) int {
	return int(d.hasher(key) & uint64(size-1))
}

func normalizeSize(n int) int {
	if n < 1 {
		n = 1
	}
	// Buckets are always a power of two so hash masking is explicit.
	size := 1
	for size < n {
		size <<= 1
	}
	return size
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
