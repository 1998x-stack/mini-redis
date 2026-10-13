package skiplist

import (
	"math/rand"
	"sort"
	"testing"
)

func TestInsertRankRangeDelete(t *testing.T) {
	s := NewWithSeed(7)
	s.Insert(2, "b")
	s.Insert(1, "a")
	s.Insert(2, "a")
	s.Insert(3, "c")
	want := []Entry{{"a", 1}, {"a", 2}, {"b", 2}, {"c", 3}}
	got := s.Entries()
	if len(got) != len(want) {
		t.Fatalf("len=%d", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%d got=%v want=%v", i, got[i], want[i])
		}
	}
	for i, e := range want {
		r, ok := s.Rank(e.Score, e.Member)
		if !ok || r != i {
			t.Fatalf("rank %v=%d,%v", e, r, ok)
		}
	}
	if !s.Delete(2, "a") {
		t.Fatal("delete")
	}
	if _, ok := s.Rank(2, "a"); ok {
		t.Fatal("deleted rank exists")
	}
}

func TestRandomOrderAgainstReference(t *testing.T) {
	type item struct {
		member string
		score  float64
	}
	r := rand.New(rand.NewSource(11))
	s := NewWithSeed(13)
	ref := make([]item, 0, 500)
	for i := 0; i < 500; i++ {
		e := item{member: string(rune(0x1000 + i)), score: float64(r.Intn(50))}
		ref = append(ref, e)
		s.Insert(e.score, e.member)
	}
	sort.Slice(ref, func(i, j int) bool {
		return ref[i].score < ref[j].score || (ref[i].score == ref[j].score && ref[i].member < ref[j].member)
	})
	got := s.Entries()
	if len(got) != len(ref) {
		t.Fatalf("len got=%d want=%d", len(got), len(ref))
	}
	for i := range ref {
		if got[i].Member != ref[i].member || got[i].Score != ref[i].score {
			t.Fatalf("at %d got=%v ref=%v", i, got[i], ref[i])
		}
		rank, ok := s.Rank(ref[i].score, ref[i].member)
		if !ok || rank != i {
			t.Fatalf("rank %d got=%d ok=%v", i, rank, ok)
		}
	}
}
