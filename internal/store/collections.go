package store

import (
	"sort"
	"strconv"

	"github.com/1998x-stack/mini-redis/internal/dict"
	"github.com/1998x-stack/mini-redis/internal/skiplist"
)

const (
	CompactMaxEntries  = 8
	QuickListChunkSize = 4
)

// ---- List: listpack-like compact slice -> chunked quicklist ----

type quickNode struct {
	values     []string
	prev, next *quickNode
}
type quickList struct {
	head, tail *quickNode
	length     int
}

func (q *quickList) pushFront(v string) {
	if q.head == nil {
		n := &quickNode{values: []string{v}}
		q.head = n
		q.tail = n
		q.length = 1
		return
	}
	if len(q.head.values) < QuickListChunkSize {
		q.head.values = append([]string{v}, q.head.values...)
		q.length++
		return
	}
	n := &quickNode{values: []string{v}, next: q.head}
	q.head.prev = n
	q.head = n
	q.length++
}
func (q *quickList) pushBack(v string) {
	if q.tail == nil {
		n := &quickNode{values: []string{v}}
		q.head = n
		q.tail = n
		q.length = 1
		return
	}
	if len(q.tail.values) < QuickListChunkSize {
		q.tail.values = append(q.tail.values, v)
		q.length++
		return
	}
	n := &quickNode{values: []string{v}, prev: q.tail}
	q.tail.next = n
	q.tail = n
	q.length++
}
func (q *quickList) popFront() (string, bool) {
	if q.head == nil {
		return "", false
	}
	v := q.head.values[0]
	q.head.values = q.head.values[1:]
	q.length--
	if len(q.head.values) == 0 {
		q.head = q.head.next
		if q.head != nil {
			q.head.prev = nil
		} else {
			q.tail = nil
		}
	}
	return v, true
}
func (q *quickList) popBack() (string, bool) {
	if q.tail == nil {
		return "", false
	}
	i := len(q.tail.values) - 1
	v := q.tail.values[i]
	q.tail.values = q.tail.values[:i]
	q.length--
	if len(q.tail.values) == 0 {
		q.tail = q.tail.prev
		if q.tail != nil {
			q.tail.next = nil
		} else {
			q.head = nil
		}
	}
	return v, true
}
func (q *quickList) values() []string {
	out := make([]string, 0, q.length)
	for n := q.head; n != nil; n = n.next {
		out = append(out, n.values...)
	}
	return out
}

func NewList() Object {
	return Object{Type: ObjectList, Encoding: EncodingListPack, payload: []string{}}
}
func (o *Object) ensureQuickList() {
	if o.Type != ObjectList || o.Encoding != EncodingListPack {
		return
	}
	items := o.payload.([]string)
	q := &quickList{}
	for _, v := range items {
		q.pushBack(v)
	}
	o.Encoding = EncodingQuickList
	o.payload = q
}
func (o *Object) ListPush(left bool, values []string) int {
	if left {
		for _, v := range values {
			if o.Encoding == EncodingListPack {
				o.payload = append([]string{v}, o.payload.([]string)...)
			} else {
				o.payload.(*quickList).pushFront(v)
			}
			if o.Encoding == EncodingListPack && len(o.payload.([]string)) > CompactMaxEntries {
				o.ensureQuickList()
			}
		}
	} else {
		for _, v := range values {
			if o.Encoding == EncodingListPack {
				o.payload = append(o.payload.([]string), v)
			} else {
				o.payload.(*quickList).pushBack(v)
			}
			if o.Encoding == EncodingListPack && len(o.payload.([]string)) > CompactMaxEntries {
				o.ensureQuickList()
			}
		}
	}
	return o.ListLen()
}
func (o *Object) ListPop(left bool) (string, bool) {
	if o.Encoding == EncodingListPack {
		items := o.payload.([]string)
		if len(items) == 0 {
			return "", false
		}
		if left {
			v := items[0]
			o.payload = items[1:]
			return v, true
		}
		i := len(items) - 1
		v := items[i]
		o.payload = items[:i]
		return v, true
	}
	q := o.payload.(*quickList)
	if left {
		return q.popFront()
	}
	return q.popBack()
}
func (o Object) ListLen() int {
	if o.Encoding == EncodingListPack {
		return len(o.payload.([]string))
	}
	return o.payload.(*quickList).length
}
func (o Object) ListRange(start, stop int) []string {
	var items []string
	if o.Encoding == EncodingListPack {
		items = append([]string(nil), o.payload.([]string)...)
	} else {
		items = o.payload.(*quickList).values()
	}
	start, stop, ok := normalizeRange(len(items), start, stop)
	if !ok {
		return []string{}
	}
	return append([]string(nil), items[start:stop+1]...)
}

// ---- Hash: listpack-like field/value pairs -> Dict ----

type compactHash struct{ entries []FieldValue }
type hashTable struct{ fields *dict.Dict[string] }

func NewHash() Object {
	return Object{Type: ObjectHash, Encoding: EncodingListPack, payload: &compactHash{}}
}
func (o *Object) upgradeHash() {
	c := o.payload.(*compactHash)
	d := dict.New[string](dict.FNV1a)
	for _, p := range c.entries {
		d.Set(p.Field, p.Value)
	}
	o.Encoding = EncodingHashTable
	o.payload = &hashTable{fields: d}
}
func (o *Object) HashSet(pairs []FieldValue) int {
	added := 0
	if o.Encoding == EncodingListPack {
		c := o.payload.(*compactHash)
		for _, p := range pairs {
			found := -1
			for i := range c.entries {
				if c.entries[i].Field == p.Field {
					found = i
					break
				}
			}
			if found >= 0 {
				c.entries[found].Value = p.Value
			} else {
				c.entries = append(c.entries, p)
				added++
			}
		}
		if len(c.entries) > CompactMaxEntries {
			o.upgradeHash()
		}
		return added
	}
	h := o.payload.(*hashTable)
	for _, p := range pairs {
		if h.fields.Set(p.Field, p.Value) {
			added++
		}
	}
	return added
}
func (o Object) HashGet(field string) (string, bool) {
	if o.Encoding == EncodingListPack {
		for _, p := range o.payload.(*compactHash).entries {
			if p.Field == field {
				return p.Value, true
			}
		}
		return "", false
	}
	return o.payload.(*hashTable).fields.Get(field)
}
func (o *Object) HashDelete(fields []string) int {
	removed := 0
	if o.Encoding == EncodingListPack {
		c := o.payload.(*compactHash)
		want := map[string]struct{}{}
		for _, f := range fields {
			want[f] = struct{}{}
		}
		out := c.entries[:0]
		for _, p := range c.entries {
			if _, ok := want[p.Field]; ok {
				removed++
				delete(want, p.Field)
				continue
			}
			out = append(out, p)
		}
		c.entries = out
		return removed
	}
	h := o.payload.(*hashTable)
	for _, f := range fields {
		if h.fields.Delete(f) {
			removed++
		}
	}
	return removed
}
func (o Object) HashLen() int {
	if o.Encoding == EncodingListPack {
		return len(o.payload.(*compactHash).entries)
	}
	return o.payload.(*hashTable).fields.Len()
}
func (o Object) HashGetAll() []FieldValue {
	var out []FieldValue
	if o.Encoding == EncodingListPack {
		out = append(out, o.payload.(*compactHash).entries...)
	} else {
		h := o.payload.(*hashTable)
		h.fields.Range(func(k, v string) bool { out = append(out, FieldValue{k, v}); return true })
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Field < out[j].Field })
	return out
}

// ---- Set: sorted intset -> Dict ----

type intSet struct{ values []int64 }
type stringSet struct{ members *dict.Dict[struct{}] }

func NewSet() Object {
	return Object{Type: ObjectSet, Encoding: EncodingIntSet, payload: &intSet{}}
}
func canonicalInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && strconv.FormatInt(n, 10) == s
}
func intIndex(values []int64, n int64) (int, bool) {
	i := sort.Search(len(values), func(i int) bool { return values[i] >= n })
	return i, i < len(values) && values[i] == n
}
func (o *Object) upgradeSet() {
	s := o.payload.(*intSet)
	d := dict.New[struct{}](dict.FNV1a)
	for _, n := range s.values {
		d.Set(strconv.FormatInt(n, 10), struct{}{})
	}
	o.Encoding = EncodingHashTable
	o.payload = &stringSet{members: d}
}
func (o *Object) SetAdd(members []string) int {
	added := 0
	if o.Encoding == EncodingIntSet {
		ints := o.payload.(*intSet)
		for i, member := range members {
			n, ok := canonicalInt(member)
			if !ok {
				o.upgradeSet()
				s := o.payload.(*stringSet)
				for _, rest := range members[i:] {
					if s.members.Set(rest, struct{}{}) {
						added++
					}
				}
				return added
			}
			idx, exists := intIndex(ints.values, n)
			if exists {
				continue
			}
			ints.values = append(ints.values, 0)
			copy(ints.values[idx+1:], ints.values[idx:])
			ints.values[idx] = n
			added++
			if len(ints.values) > CompactMaxEntries {
				o.upgradeSet()
				s := o.payload.(*stringSet)
				for _, rest := range members[i+1:] {
					if s.members.Set(rest, struct{}{}) {
						added++
					}
				}
				return added
			}
		}
		return added
	}
	s := o.payload.(*stringSet)
	for _, member := range members {
		if s.members.Set(member, struct{}{}) {
			added++
		}
	}
	return added
}
func (o *Object) SetRemove(members []string) int {
	removed := 0
	if o.Encoding == EncodingIntSet {
		s := o.payload.(*intSet)
		for _, m := range members {
			n, ok := canonicalInt(m)
			if !ok {
				continue
			}
			i, exists := intIndex(s.values, n)
			if !exists {
				continue
			}
			s.values = append(s.values[:i], s.values[i+1:]...)
			removed++
		}
		return removed
	}
	s := o.payload.(*stringSet)
	for _, m := range members {
		if s.members.Delete(m) {
			removed++
		}
	}
	return removed
}
func (o Object) SetIsMember(member string) bool {
	if o.Encoding == EncodingIntSet {
		n, ok := canonicalInt(member)
		if !ok {
			return false
		}
		_, exists := intIndex(o.payload.(*intSet).values, n)
		return exists
	}
	_, ok := o.payload.(*stringSet).members.Get(member)
	return ok
}
func (o Object) SetCard() int {
	if o.Encoding == EncodingIntSet {
		return len(o.payload.(*intSet).values)
	}
	return o.payload.(*stringSet).members.Len()
}
func (o Object) SetMembers() []string {
	out := make([]string, 0, o.SetCard())
	if o.Encoding == EncodingIntSet {
		for _, n := range o.payload.(*intSet).values {
			out = append(out, strconv.FormatInt(n, 10))
		}
	} else {
		out = o.payload.(*stringSet).members.Keys(0)
		sort.Strings(out)
	}
	return out
}

// ---- ZSet: sorted compact slice -> Dict + SkipList ----

type compactZSet struct{ entries []ScoredMember }
type largeZSet struct {
	scores *dict.Dict[float64]
	order  *skiplist.SkipList
}

func NewZSet() Object {
	return Object{Type: ObjectZSet, Encoding: EncodingListPack, payload: &compactZSet{}}
}
func scoreLess(a, b ScoredMember) bool {
	return a.Score < b.Score || (a.Score == b.Score && a.Member < b.Member)
}
func sortZ(entries []ScoredMember) {
	sort.Slice(entries, func(i, j int) bool { return scoreLess(entries[i], entries[j]) })
}
func (o *Object) upgradeZSet() {
	c := o.payload.(*compactZSet)
	z := &largeZSet{scores: dict.New[float64](dict.FNV1a), order: skiplist.New()}
	for _, e := range c.entries {
		z.scores.Set(e.Member, e.Score)
		z.order.Insert(e.Score, e.Member)
	}
	o.Encoding = EncodingSkipList
	o.payload = z
}
func (o *Object) ZAdd(pairs []ScoredMember) int {
	added := 0
	if o.Encoding == EncodingListPack {
		c := o.payload.(*compactZSet)
		for _, p := range pairs {
			idx := -1
			for i := range c.entries {
				if c.entries[i].Member == p.Member {
					idx = i
					break
				}
			}
			if idx >= 0 {
				c.entries[idx].Score = p.Score
			} else {
				c.entries = append(c.entries, p)
				added++
			}
		}
		sortZ(c.entries)
		if len(c.entries) > CompactMaxEntries {
			o.upgradeZSet()
		}
		return added
	}
	z := o.payload.(*largeZSet)
	for _, p := range pairs {
		old, ok := z.scores.Get(p.Member)
		if ok {
			if old == p.Score {
				continue
			}
			z.order.Delete(old, p.Member)
		} else {
			added++
		}
		z.scores.Set(p.Member, p.Score)
		z.order.Insert(p.Score, p.Member)
	}
	return added
}
func (o Object) ZScore(member string) (float64, bool) {
	if o.Encoding == EncodingListPack {
		for _, e := range o.payload.(*compactZSet).entries {
			if e.Member == member {
				return e.Score, true
			}
		}
		return 0, false
	}
	return o.payload.(*largeZSet).scores.Get(member)
}
func (o Object) ZRank(member string) (int, bool) {
	score, ok := o.ZScore(member)
	if !ok {
		return 0, false
	}
	if o.Encoding == EncodingListPack {
		for i, e := range o.payload.(*compactZSet).entries {
			if e.Member == member {
				return i, true
			}
		}
		return 0, false
	}
	return o.payload.(*largeZSet).order.Rank(score, member)
}
func (o Object) ZRange(start, stop int) []ScoredMember {
	n := o.ZLen()
	start, stop, ok := normalizeRange(n, start, stop)
	if !ok {
		return []ScoredMember{}
	}
	if o.Encoding == EncodingListPack {
		return append([]ScoredMember(nil), o.payload.(*compactZSet).entries[start:stop+1]...)
	}
	entries := o.payload.(*largeZSet).order.Range(start, stop)
	out := make([]ScoredMember, len(entries))
	for i, e := range entries {
		out[i] = ScoredMember{Member: e.Member, Score: e.Score}
	}
	return out
}
func (o *Object) ZRemove(members []string) int {
	removed := 0
	if o.Encoding == EncodingListPack {
		c := o.payload.(*compactZSet)
		want := map[string]struct{}{}
		for _, m := range members {
			want[m] = struct{}{}
		}
		out := c.entries[:0]
		for _, e := range c.entries {
			if _, ok := want[e.Member]; ok {
				removed++
				delete(want, e.Member)
				continue
			}
			out = append(out, e)
		}
		c.entries = out
		return removed
	}
	z := o.payload.(*largeZSet)
	for _, m := range members {
		score, ok := z.scores.Get(m)
		if !ok {
			continue
		}
		z.scores.Delete(m)
		z.order.Delete(score, m)
		removed++
	}
	return removed
}
func (o Object) ZLen() int {
	if o.Encoding == EncodingListPack {
		return len(o.payload.(*compactZSet).entries)
	}
	return o.payload.(*largeZSet).scores.Len()
}

func normalizeRange(length, start, stop int) (int, int, bool) {
	if length == 0 {
		return 0, 0, false
	}
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= length || start > stop {
		return 0, 0, false
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop, true
}
