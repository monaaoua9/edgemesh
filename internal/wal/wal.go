// Package wal is a small segmented append-only log. Records get a dense
// uint64 index, are CRC'd, and survive torn writes at the tail.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerSize          = 8 // len u32, crc u32
	segExt              = ".wal"
	MaxRecordSize       = 1 << 20
	DefaultSegmentBytes = 16 << 20
)

type SyncMode int

const (
	SyncInterval SyncMode = iota // fsync on a timer, loses at most SyncEvery on power cut
	SyncAlways                   // fsync every append / batch
	SyncNever                    // leave it to the OS
)

var (
	ErrClosed   = errors.New("wal: closed")
	ErrTooLarge = errors.New("wal: record too large")
	ErrNoData   = errors.New("wal: no more data")
	ErrCorrupt  = errors.New("wal: corrupt record")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Options struct {
	SegmentBytes int64
	MaxBytes     int64 // 0 = unbounded; otherwise oldest segments are dropped
	Sync         SyncMode
	SyncEvery    time.Duration
	Logger       *slog.Logger
}

type segment struct {
	base uint64
	path string
	size int64
}

type Stats struct {
	FirstIndex uint64
	NextIndex  uint64
	Segments   int
	Bytes      int64
	Dropped    uint64 // records evicted by MaxBytes before they were forwarded
	Appended   uint64
}

type Log struct {
	dir  string
	opts Options
	lg   *slog.Logger

	mu     sync.Mutex
	segs   []segment
	active *os.File
	next   uint64
	dirty  bool
	closed bool
	wbuf   []byte

	dropped  uint64
	appended uint64

	notify chan struct{}
	stop   chan struct{}
	wg     sync.WaitGroup
}

func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if opts.SyncEvery <= 0 {
		opts.SyncEvery = 100 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	l := &Log{
		dir:    dir,
		opts:   opts,
		lg:     opts.Logger.With("component", "wal"),
		notify: make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
	if err := l.load(); err != nil {
		return nil, err
	}

	if opts.Sync == SyncInterval {
		l.wg.Add(1)
		go l.syncLoop()
	}
	return l, nil
}

func segName(base uint64) string { return fmt.Sprintf("%020d%s", base, segExt) }

func (l *Log) load() error {
	ents, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segExt) {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, segExt), 10, 64)
		if err != nil {
			continue
		}
		st, err := e.Info()
		if err != nil {
			return err
		}
		l.segs = append(l.segs, segment{base: base, path: filepath.Join(l.dir, name), size: st.Size()})
	}
	sort.Slice(l.segs, func(i, j int) bool { return l.segs[i].base < l.segs[j].base })

	if len(l.segs) == 0 {
		return l.newSegment(0)
	}

	last := &l.segs[len(l.segs)-1]
	valid, count, err := scanTail(last.path)
	if err != nil {
		return err
	}
	if valid != last.size {
		l.lg.Warn("truncating torn tail", "segment", last.path, "from", last.size, "to", valid)
		if err := os.Truncate(last.path, valid); err != nil {
			return err
		}
		last.size = valid
	}
	l.next = last.base + count

	f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.active = f
	return nil
}

// walks the segment and returns the size of the valid prefix and its record count
func scanTail(path string) (valid int64, count uint64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var off int64
	for {
		if int64(len(data))-off < headerSize {
			return off, count, nil
		}
		n := int64(binary.BigEndian.Uint32(data[off:]))
		sum := binary.BigEndian.Uint32(data[off+4:])
		end := off + headerSize + n
		if n > MaxRecordSize || end > int64(len(data)) {
			return off, count, nil
		}
		if crc32.Checksum(data[off+headerSize:end], castagnoli) != sum {
			return off, count, nil
		}
		off = end
		count++
	}
}

func (l *Log) newSegment(base uint64) error {
	path := filepath.Join(l.dir, segName(base))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	l.active = f
	l.segs = append(l.segs, segment{base: base, path: path})
	l.next = base
	syncDir(l.dir)
	return nil
}

func (l *Log) Append(p []byte) (uint64, error) {
	return l.AppendBatch([][]byte{p})
}

// AppendBatch writes all records with a single write (and at most one fsync).
// Returns the index of the first record.
func (l *Log) AppendBatch(recs [][]byte) (uint64, error) {
	total := 0
	for _, r := range recs {
		if len(r) > MaxRecordSize {
			return 0, ErrTooLarge
		}
		total += headerSize + len(r)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}

	cur := &l.segs[len(l.segs)-1]
	if cur.size > 0 && cur.size+int64(total) > l.opts.SegmentBytes {
		if err := l.roll(); err != nil {
			return 0, err
		}
		cur = &l.segs[len(l.segs)-1]
	}

	buf := l.wbuf[:0]
	for _, r := range recs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r)))
		buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(r, castagnoli))
		buf = append(buf, r...)
	}
	l.wbuf = buf[:0]
	if cap(l.wbuf) > 4<<20 {
		l.wbuf = nil // don't pin a huge buffer forever
	}

	if _, err := l.active.Write(buf); err != nil {
		// a short write leaves garbage mid-segment; cut it back
		_ = l.active.Truncate(cur.size)
		return 0, err
	}
	first := l.next
	cur.size += int64(len(buf))
	l.next += uint64(len(recs))
	l.appended += uint64(len(recs))
	l.dirty = true

	if l.opts.Sync == SyncAlways {
		if err := l.active.Sync(); err != nil {
			return first, err
		}
		l.dirty = false
	}

	select {
	case l.notify <- struct{}{}:
	default:
	}
	return first, nil
}

// caller holds mu
func (l *Log) roll() error {
	if err := l.active.Sync(); err != nil {
		return err
	}
	if err := l.active.Close(); err != nil {
		return err
	}
	l.dirty = false
	if err := l.newSegment(l.next); err != nil {
		return err
	}
	l.enforceRetention()
	return nil
}

func (l *Log) enforceRetention() {
	if l.opts.MaxBytes <= 0 {
		return
	}
	for len(l.segs) > 1 && l.totalBytes() > l.opts.MaxBytes {
		old := l.segs[0]
		if err := os.Remove(old.path); err != nil {
			// windows: reader still has it open. try again on the next roll
			l.lg.Debug("retention remove failed", "err", err)
			return
		}
		l.dropped += l.segs[1].base - old.base
		l.lg.Warn("retention dropped segment", "base", old.base, "records", l.segs[1].base-old.base)
		l.segs = l.segs[1:]
	}
}

func (l *Log) totalBytes() int64 {
	var n int64
	for _, s := range l.segs {
		n += s.size
	}
	return n
}

// TruncateBefore deletes whole segments that only hold records < idx.
func (l *Log) TruncateBefore(idx uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for len(l.segs) > 1 && l.segs[1].base <= idx {
		if err := os.Remove(l.segs[0].path); err != nil {
			l.lg.Debug("truncate remove failed", "err", err)
			return
		}
		l.segs = l.segs[1:]
	}
}

func (l *Log) Notify() <-chan struct{} { return l.notify }

func (l *Log) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Stats{
		FirstIndex: l.segs[0].base,
		NextIndex:  l.next,
		Segments:   len(l.segs),
		Bytes:      l.totalBytes(),
		Dropped:    l.dropped,
		Appended:   l.appended,
	}
}

func (l *Log) snapshot() []segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]segment(nil), l.segs...)
}

// used by readers that hit corruption in the active segment
func (l *Log) rollIfActive(base uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.segs[len(l.segs)-1].base != base {
		return
	}
	if err := l.roll(); err != nil {
		l.lg.Error("forced roll failed", "err", err)
	}
}

func (l *Log) Sync() error {
	l.mu.Lock()
	f := l.active
	l.dirty = false
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return f.Sync()
}

func (l *Log) syncLoop() {
	defer l.wg.Done()
	t := time.NewTicker(l.opts.SyncEvery)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.mu.Lock()
			f, d := l.active, l.dirty
			l.dirty = false
			l.mu.Unlock()
			if d {
				if err := f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
					l.lg.Error("fsync failed", "err", err)
				}
			}
		}
	}
}

func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.stop)
	l.mu.Unlock()

	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.active.Sync()
	if cerr := l.active.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) {
	// no-op on windows (can't open dirs), best effort elsewhere
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	f.Close()
}
