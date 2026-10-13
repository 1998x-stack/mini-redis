package store

import "testing"

func TestListEncodingConversionPreservesOrder(t *testing.T) {
	o := NewList()
	o.ListPush(false, []string{"a", "b", "c", "d", "e", "f", "g", "h"})
	if o.Encoding != EncodingListPack {
		t.Fatalf("encoding=%v", o.Encoding)
	}
	o.ListPush(false, []string{"i"})
	if o.Encoding != EncodingQuickList {
		t.Fatalf("encoding=%v", o.Encoding)
	}
	got := o.ListRange(0, -1)
	if len(got) != 9 || got[0] != "a" || got[8] != "i" {
		t.Fatalf("got=%v", got)
	}
	o.ListPush(true, []string{"x", "y"})
	got = o.ListRange(0, 2)
	if got[0] != "y" || got[1] != "x" || got[2] != "a" {
		t.Fatalf("LPUSH order=%v", got)
	}
}

func TestHashConversionPreservesValues(t *testing.T) {
	o := NewHash()
	pairs := make([]FieldValue, 0, 9)
	for i := 0; i < 9; i++ {
		pairs = append(pairs, FieldValue{Field: string(rune('a' + i)), Value: string(rune('0' + i))})
	}
	if n := o.HashSet(pairs); n != 9 {
		t.Fatal(n)
	}
	if o.Encoding != EncodingHashTable {
		t.Fatal(o.Encoding)
	}
	if v, ok := o.HashGet("e"); !ok || v != "4" {
		t.Fatalf("%q %v", v, ok)
	}
	if n := o.HashSet([]FieldValue{{Field: "e", Value: "updated"}}); n != 0 {
		t.Fatal(n)
	}
}

func TestSetIntSetUpgrade(t *testing.T) {
	o := NewSet()
	if n := o.SetAdd([]string{"3", "1", "2"}); n != 3 {
		t.Fatal(n)
	}
	if o.Encoding != EncodingIntSet {
		t.Fatal(o.Encoding)
	}
	if got := o.SetMembers(); len(got) != 3 || got[0] != "1" || got[2] != "3" {
		t.Fatal(got)
	}
	if n := o.SetAdd([]string{"01"}); n != 1 {
		t.Fatal(n)
	}
	if o.Encoding != EncodingHashTable {
		t.Fatal(o.Encoding)
	}
	if !o.SetIsMember("01") || !o.SetIsMember("1") {
		t.Fatal(o.SetMembers())
	}
}

func TestZSetSkipListUpgradeAndRank(t *testing.T) {
	o := NewZSet()
	pairs := []ScoredMember{{"a", 4}, {"b", 1}, {"c", 3}, {"d", 2}, {"e", 5}, {"f", 6}, {"g", 7}, {"h", 8}, {"i", 9}}
	if n := o.ZAdd(pairs); n != 9 {
		t.Fatal(n)
	}
	if o.Encoding != EncodingSkipList {
		t.Fatal(o.Encoding)
	}
	rank, ok := o.ZRank("a")
	if !ok || rank != 3 {
		t.Fatalf("rank=%d ok=%v", rank, ok)
	}
	if n := o.ZAdd([]ScoredMember{{"a", 0.5}}); n != 0 {
		t.Fatal(n)
	}
	rank, _ = o.ZRank("a")
	if rank != 0 {
		t.Fatalf("rank after update=%d", rank)
	}
	got := o.ZRange(0, 2)
	if len(got) != 3 || got[0].Member != "a" || got[1].Member != "b" {
		t.Fatal(got)
	}
}
