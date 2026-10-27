package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T, dir string, o Options) *Log {
	t.Helper()
	l, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func drain(t *testing.T, r *Reader) []string {
	t.Helper()
	var out []string
	for {
		_, p, err := r.Next()
		if errors.Is(err, ErrNoData) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(p))
	}
}

func TestAppendRead(t *testing.T) {
	l := open(t, t.TempDir(), Options{})
	for i := range 10 {
		idx, err := l.Append([]byte(fmt.Sprintf("rec-%d", i)))
		if err != nil || idx != uint64(i) {
			t.Fatalf("append %d: idx=%d err=%v", i, idx, err)
		}
	}
	r := l.NewReader(0)
	defer r.Close()
	got := drain(t, r)
	if len(got) != 10 || got[0] != "rec-0" || got[9] != "rec-9" {
		t.Fatalf("got %v", got)
	}

	r2 := l.NewReader(7)
	defer r2.Close()
	if got := drain(t, r2); len(got) != 3 || got[0] != "rec-7" {
		t.Fatalf("from 7: %v", got)
	}
}

func TestRotationAndOrder(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SegmentBytes: 256})
	const n = 200
	for i := range n {
		if _, err := l.Append([]byte(fmt.Sprintf("record-number-%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if l.Stats().Segments < 3 {
		t.Fatalf("expected rotation, got %d segments", l.Stats().Segments)
	}
	r := l.NewReader(0)
	defer r.Close()
	got := drain(t, r)
	if len(got) != n {
		t.Fatalf("got %d records", len(got))
	}
	for i, s := range got {
		if s != fmt.Sprintf("record-number-%04d", i) {
			t.Fatalf("out of order at %d: %s", i, s)
		}
	}
}

func TestReopenContinuesIndex(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{SegmentBytes: 512})
	for i := range 50 {
		l.Append([]byte(fmt.Sprintf("a%d", i)))
	}
	l.Close()

	l2 := open(t, dir, Options{SegmentBytes: 512})
	idx, err := l2.Append([]byte("next"))
	if err != nil || idx != 50 {
		t.Fatalf("idx=%d err=%v", idx, err)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{})
	for i := range 5 {
		l.Append([]byte(fmt.Sprintf("good-%d", i)))
	}
	l.Close()

	path := filepath.Join(dir, segName(0))
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 40, 1, 2, 3, 4, 'h', 'a', 'l', 'f'}) // header says 40 bytes, only 4 follow
	f.Close()

	l2 := open(t, dir, Options{})
	if l2.Stats().NextIndex != 5 {
		t.Fatalf("next=%d", l2.Stats().NextIndex)
	}
	l2.Append([]byte("after"))
	r := l2.NewReader(0)
	defer r.Close()
	got := drain(t, r)
	if len(got) != 6 || got[5] != "after" {
		t.Fatalf("got %v", got)
	}
}

func TestCorruptCRCStopsRecovery(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{})
	for i := range 4 {
		l.Append([]byte(fmt.Sprintf("payload-%d", i)))
	}
	l.Close()

	path := filepath.Join(dir, segName(0))
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // flip a bit in the last record
	os.WriteFile(path, b, 0o644)

	l2 := open(t, dir, Options{})
	if l2.Stats().NextIndex != 3 {
		t.Fatalf("next=%d, want 3", l2.Stats().NextIndex)
	}
}

func TestTruncateBefore(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentBytes: 128})
	for i := range 100 {
		l.Append([]byte(fmt.Sprintf("r%03d", i)))
	}
	before := l.Stats().Segments
	l.TruncateBefore(80)
	after := l.Stats()
	if after.Segments >= before {
		t.Fatalf("nothing deleted: %d -> %d", before, after.Segments)
	}
	if after.FirstIndex > 80 {
		t.Fatalf("deleted too much: first=%d", after.FirstIndex)
	}
	r := l.NewReader(80)
	defer r.Close()
	if got := drain(t, r); len(got) != 20 || got[0] != "r080" {
		t.Fatalf("got %d, first %q", len(got), got[0])
	}
}

func TestRetentionDropsOldest(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentBytes: 256, MaxBytes: 1024})
	for i := range 300 {
		l.Append([]byte(fmt.Sprintf("record-%04d", i)))
	}
	s := l.Stats()
	if s.Dropped == 0 {
		t.Fatal("expected drops")
	}
	if s.Bytes > 1024+256 {
		t.Fatalf("bytes=%d", s.Bytes)
	}
	r := l.NewReader(0)
	defer r.Close()
	got := drain(t, r)
	if r.Lost == 0 {
		t.Fatal("reader should report lost records")
	}
	if got[len(got)-1] != "record-0299" {
		t.Fatalf("last=%s", got[len(got)-1])
	}
}

func TestBatchAndTooLarge(t *testing.T) {
	l := open(t, t.TempDir(), Options{Sync: SyncAlways})
	first, err := l.AppendBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil || first != 0 {
		t.Fatal(first, err)
	}
	if _, err := l.Append(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if l.Stats().NextIndex != 3 {
		t.Fatal("index advanced on failed append")
	}
}

func TestTailingReaderSeesLiveWrites(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentBytes: 200})
	r := l.NewReader(0)
	defer r.Close()

	const n = 500
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range n {
			l.Append([]byte(fmt.Sprintf("live-%d", i)))
		}
	}()

	deadline := time.After(10 * time.Second)
	for want := 0; want < n; {
		idx, p, err := r.Next()
		if errors.Is(err, ErrNoData) {
			select {
			case <-l.Notify():
			case <-time.After(5 * time.Millisecond):
			case <-deadline:
				t.Fatalf("timed out at %d", want)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if idx != uint64(want) || !bytes.Equal(p, []byte(fmt.Sprintf("live-%d", want))) {
			t.Fatalf("want %d got idx=%d %q", want, idx, p)
		}
		want++
	}
	wg.Wait()
}

func TestConcurrentAppenders(t *testing.T) {
	l := open(t, t.TempDir(), Options{SegmentBytes: 4096})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 250 {
				if _, err := l.Append([]byte(fmt.Sprintf("g%d-%d", g, i))); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	r := l.NewReader(0)
	defer r.Close()
	if got := drain(t, r); len(got) != 2000 {
		t.Fatalf("got %d", len(got))
	}
}

func BenchmarkAppend(b *testing.B) {
	for _, mode := range []struct {
		name string
		m    SyncMode
	}{{"never", SyncNever}, {"interval", SyncInterval}} {
		b.Run(mode.name, func(b *testing.B) {
			l, _ := Open(b.TempDir(), Options{Sync: mode.m})
			defer l.Close()
			rec := make([]byte, 48)
			b.SetBytes(int64(len(rec)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l.Append(rec)
			}
		})
	}
}

func BenchmarkAppendBatch64(b *testing.B) {
	l, _ := Open(b.TempDir(), Options{})
	defer l.Close()
	recs := make([][]byte, 64)
	for i := range recs {
		recs[i] = make([]byte, 48)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.AppendBatch(recs)
	}
}
