// Package skiplist implements the ordered index used by mini-redis sorted sets.
// It mirrors the core Redis idea: probabilistic levels provide logarithmic
// search while per-level spans make rank lookup efficient.
package skiplist

import "math/rand"

const (
	maxLevel    = 16
	probability = 0.25
)

type level struct {
	forward *node
	span    int
}

type node struct {
	member   string
	score    float64
	backward *node
	levels   []level
}

// Entry is the public immutable view used by zset code and tests.
type Entry struct {
	Member string
	Score  float64
}

// SkipList orders by score first and member lexicographically as a tie-breaker.
type SkipList struct {
	header *node
	tail   *node
	level  int
	length int
	rng    *rand.Rand
}

func New() *SkipList { return NewWithSeed(1) }

func NewWithSeed(seed int64) *SkipList {
	return &SkipList{
		header: &node{levels: make([]level, maxLevel)},
		level:  1,
		rng:    rand.New(rand.NewSource(seed)),
	}
}

func (s *SkipList) Len() int { return s.length }

func less(score float64, member string, n *node) bool {
	return n.score < score || (n.score == score && n.member < member)
}

func equal(n *node, score float64, member string) bool {
	return n != nil && n.score == score && n.member == member
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && s.rng.Float64() < probability {
		level++
	}
	return level
}

// Insert adds a unique (score, member) pair. Callers that change a member's
// score delete the old pair first, matching the zset dictionary+skiplist model.
func (s *SkipList) Insert(score float64, member string) {
	update := make([]*node, maxLevel)
	rank := make([]int, maxLevel)
	x := s.header

	for i := s.level - 1; i >= 0; i-- {
		if i == s.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.levels[i].forward != nil && less(score, member, x.levels[i].forward) {
			rank[i] += x.levels[i].span
			x = x.levels[i].forward
		}
		update[i] = x
	}

	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			rank[i] = 0
			update[i] = s.header
			update[i].levels[i].span = s.length
		}
		s.level = newLevel
	}

	x = &node{member: member, score: score, levels: make([]level, newLevel)}
	for i := 0; i < newLevel; i++ {
		x.levels[i].forward = update[i].levels[i].forward
		updateSpan := update[i].levels[i].span
		distance := rank[0] - rank[i]
		x.levels[i].span = updateSpan - distance
		update[i].levels[i].forward = x
		update[i].levels[i].span = distance + 1
	}
	for i := newLevel; i < s.level; i++ {
		update[i].levels[i].span++
	}

	if update[0] == s.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.levels[0].forward != nil {
		x.levels[0].forward.backward = x
	} else {
		s.tail = x
	}
	s.length++
}

func (s *SkipList) Delete(score float64, member string) bool {
	update := make([]*node, maxLevel)
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil && less(score, member, x.levels[i].forward) {
			x = x.levels[i].forward
		}
		update[i] = x
	}
	x = x.levels[0].forward
	if !equal(x, score, member) {
		return false
	}

	for i := 0; i < s.level; i++ {
		if update[i].levels[i].forward == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].forward = x.levels[i].forward
		} else {
			update[i].levels[i].span--
		}
	}
	if x.levels[0].forward != nil {
		x.levels[0].forward.backward = x.backward
	} else {
		s.tail = x.backward
	}
	for s.level > 1 && s.header.levels[s.level-1].forward == nil {
		s.level--
	}
	s.length--
	return true
}

// Rank returns the zero-based rank of an exact score/member pair.
func (s *SkipList) Rank(score float64, member string) (int, bool) {
	rank := 0
	x := s.header
	for i := s.level - 1; i >= 0; i-- {
		for x.levels[i].forward != nil {
			next := x.levels[i].forward
			if next.score < score || (next.score == score && next.member <= member) {
				rank += x.levels[i].span
				x = next
				if equal(x, score, member) {
					return rank - 1, true
				}
				continue
			}
			break
		}
	}
	return 0, false
}

// Range returns inclusive zero-based ranks. Negative indices are normalized in
// the zset layer so this primitive stays focused on ordered traversal.
func (s *SkipList) Range(start, stop int) []Entry {
	if start < 0 || stop < start || start >= s.length {
		return nil
	}
	if stop >= s.length {
		stop = s.length - 1
	}
	out := make([]Entry, 0, stop-start+1)
	idx := 0
	x := s.header.levels[0].forward
	for x != nil && idx <= stop {
		if idx >= start {
			out = append(out, Entry{Member: x.member, Score: x.score})
		}
		idx++
		x = x.levels[0].forward
	}
	return out
}

func (s *SkipList) Entries() []Entry { return s.Range(0, s.length-1) }
