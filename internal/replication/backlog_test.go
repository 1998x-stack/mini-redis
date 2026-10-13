package replication

import "testing"

func TestBacklogRetainsWholeFramesAndResumesAtBoundaries(t *testing.T) {
	b := NewBacklog(10)
	f1 := b.Append([]byte("aaaa"))
	f2 := b.Append([]byte("bbb"))
	f3 := b.Append([]byte("ccccc"))

	if f1.Start != 1 || f1.End != 4 || f2.Start != 5 || f2.End != 7 || f3.Start != 8 || f3.End != 12 {
		t.Fatalf("unexpected offsets: %#v %#v %#v", f1, f2, f3)
	}
	frames := b.SnapshotFrames()
	if len(frames) != 2 || string(frames[0].Data) != "bbb" || string(frames[1].Data) != "ccccc" {
		t.Fatalf("frames after wrap=%#v", frames)
	}
	if got, ok := b.SuffixAfter(7); !ok || string(got) != "ccccc" {
		t.Fatalf("suffix after 7=(%q,%v)", got, ok)
	}
	if _, ok := b.SuffixAfter(4); ok {
		t.Fatal("offset 4 should be outside retained backlog")
	}
	if _, ok := b.SuffixAfter(9); ok {
		t.Fatal("interior frame offset must not be resumable")
	}
	if got, ok := b.SuffixAfter(12); !ok || len(got) != 0 {
		t.Fatalf("current offset should resume empty, got=%q ok=%v", got, ok)
	}
}

func TestOversizedFrameAdvancesOffsetButIsNotPartiallyRetained(t *testing.T) {
	b := NewBacklog(4)
	f := b.Append([]byte("123456"))
	if f.End != 6 || b.EndOffset() != 6 {
		t.Fatalf("offset=%d frame=%#v", b.EndOffset(), f)
	}
	if len(b.SnapshotFrames()) != 0 {
		t.Fatal("oversized frame must not be partially retained")
	}
	if _, ok := b.SuffixAfter(0); ok {
		t.Fatal("cannot resume before an oversized non-retained frame")
	}
	if got, ok := b.SuffixAfter(6); !ok || len(got) != 0 {
		t.Fatalf("current end should remain resumable, got=%q ok=%v", got, ok)
	}
}
