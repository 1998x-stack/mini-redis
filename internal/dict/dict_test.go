package dict

import (
	"fmt"
	"math/rand"
	"testing"
)

func constantHash(string) uint64 { return 0 }

func TestDictBasicInsertUpdateDeleteWithCollisions(t *testing.T) {
	d := NewWithSize[int](4, constantHash)
	for i := 0; i < 4; i++ {
		if inserted := d.Set(fmt.Sprintf("k%d", i), i); !inserted {
			t.Fatalf("insert k%d reported update", i)
		}
	}
	if got := d.Len(); got != 4 {
		t.Fatalf("Len=%d want 4", got)
	}
	if inserted := d.Set("k2", 99); inserted {
		t.Fatal("updating existing key reported insert")
	}
	if got, ok := d.Get("k2"); !ok || got != 99 {
		t.Fatalf("Get(k2)=(%d,%v) want (99,true)", got, ok)
	}
	if !d.Delete("k1") || d.Delete("k1") {
		t.Fatal("delete result mismatch")
	}
	if _, ok := d.Get("k1"); ok {
		t.Fatal("deleted key still visible")
	}
}

func TestRehashStepMovesAtMostOneCollidingEntryPerUnit(t *testing.T) {
	d := NewWithSize[int](4, constantHash)
	for i := 0; i < 5; i++ {
		d.Set(fmt.Sprintf("k%d", i), i)
	}
	if !d.IsRehashing() {
		t.Fatal("expected growth rehash after crossing load factor 1")
	}
	before := d.Stats()
	if before.Table0Used != 4 || before.Table1Used != 1 {
		t.Fatalf("unexpected pre-step stats: %#v", before)
	}

	used := d.RehashStep(1)
	after := d.Stats()
	if used != 1 {
		t.Fatalf("used=%d want 1", used)
	}
	if after.Table0Used != before.Table0Used-1 {
		t.Fatalf("one work unit moved %d old entries; stats before=%#v after=%#v", before.Table0Used-after.Table0Used, before, after)
	}
	if after.Table1Used != before.Table1Used+1 {
		t.Fatalf("new table did not gain exactly one entry: before=%#v after=%#v", before, after)
	}
	if after.RehashIndex != 0 {
		t.Fatalf("collision chain should keep rehash index at bucket 0, got %d", after.RehashIndex)
	}
}

func TestLookupUpdateDeleteRemainCorrectAcrossBothTables(t *testing.T) {
	d := NewWithSize[int](4, constantHash)
	for i := 0; i < 5; i++ {
		d.Set(fmt.Sprintf("k%d", i), i)
	}

	// Move one old entry into ht[1]. With head insertion and constant hashing,
	// k3 is the first old entry migrated.
	d.RehashStep(1)
	if d.findEntry(1, "k3") == nil {
		t.Fatal("expected k3 in target table after one migration unit")
	}
	if d.findEntry(0, "k0") == nil {
		t.Fatal("expected k0 to remain in source table")
	}

	beforeLen := d.Len()
	if inserted := d.Set("k3", 300); inserted || d.Len() != beforeLen {
		t.Fatalf("update in ht[1] duplicated key; inserted=%v len=%d before=%d", inserted, d.Len(), beforeLen)
	}
	if inserted := d.Set("k0", 100); inserted || d.Len() != beforeLen {
		t.Fatalf("update in ht[0] duplicated key; inserted=%v len=%d before=%d", inserted, d.Len(), beforeLen)
	}
	if got, ok := d.Peek("k3"); !ok || got != 300 {
		t.Fatalf("k3=(%d,%v)", got, ok)
	}
	if got, ok := d.Peek("k0"); !ok || got != 100 {
		t.Fatalf("k0=(%d,%v)", got, ok)
	}

	if !d.Delete("k3") || !d.Delete("k0") {
		t.Fatal("delete must work from either rehash table")
	}
	if _, ok := d.Peek("k3"); ok {
		t.Fatal("k3 survived delete")
	}
	if _, ok := d.Peek("k0"); ok {
		t.Fatal("k0 survived delete")
	}
}

func TestRangeDuringRehashVisitsEachLogicalKeyOnce(t *testing.T) {
	d := NewWithSize[int](4, FNV1a)
	for i := 0; i < 5; i++ {
		d.Set(fmt.Sprintf("k%d", i), i)
	}
	if !d.IsRehashing() {
		t.Fatal("expected active rehash")
	}
	d.RehashStep(2)

	seen := map[string]int{}
	d.Range(func(key string, value int) bool {
		seen[key]++
		return true
	})
	if len(seen) != d.Len() {
		t.Fatalf("visited %d unique keys, Len=%d", len(seen), d.Len())
	}
	for key, count := range seen {
		if count != 1 {
			t.Fatalf("key %q visited %d times", key, count)
		}
	}
}

func TestGrowCompletesAndSparseTableShrinks(t *testing.T) {
	d := NewWithSize[int](4, FNV1a)
	for i := 0; i < 40; i++ {
		d.Set(fmt.Sprintf("k%02d", i), i)
	}
	for d.IsRehashing() {
		d.RehashStep(64)
	}
	grown := d.Stats().Table0Size
	if grown <= 4 {
		t.Fatalf("table did not grow: size=%d", grown)
	}

	for i := 0; i < 39; i++ {
		d.Delete(fmt.Sprintf("k%02d", i))
		for d.IsRehashing() {
			d.RehashStep(64)
		}
	}
	if got := d.Len(); got != 1 {
		t.Fatalf("Len=%d want 1", got)
	}
	if size := d.Stats().Table0Size; size >= grown {
		t.Fatalf("sparse dictionary did not shrink: grown=%d now=%d", grown, size)
	}
}

func TestRandomizedOperationsMatchGoMapDuringRehash(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	d := NewWithSize[int](4, FNV1a)
	model := map[string]int{}

	for step := 0; step < 5000; step++ {
		key := fmt.Sprintf("k%d", rng.Intn(128))
		switch rng.Intn(4) {
		case 0, 1:
			value := rng.Intn(1_000_000)
			d.Set(key, value)
			model[key] = value
		case 2:
			got := d.Delete(key)
			_, existed := model[key]
			if got != existed {
				t.Fatalf("step %d Delete(%q)=%v model existed=%v", step, key, got, existed)
			}
			delete(model, key)
		case 3:
			got, ok := d.Get(key)
			want, exists := model[key]
			if ok != exists || (ok && got != want) {
				t.Fatalf("step %d Get(%q)=(%d,%v) want=(%d,%v)", step, key, got, ok, want, exists)
			}
		}

		// Simulate a small event-loop maintenance budget in addition to the
		// opportunistic work performed by dictionary operations.
		if step%11 == 0 {
			d.RehashStep(2)
		}
		if d.Len() != len(model) {
			t.Fatalf("step %d Len=%d model=%d stats=%#v", step, d.Len(), len(model), d.Stats())
		}
	}

	for key, want := range model {
		got, ok := d.Peek(key)
		if !ok || got != want {
			t.Fatalf("final key %q=(%d,%v) want=%d", key, got, ok, want)
		}
	}
}
