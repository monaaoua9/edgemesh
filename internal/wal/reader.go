package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

const readChunk = 64 << 10

var errShort = errors.New("short")

// Reader is a cursor over the log. One goroutine only. Closing it matters on
// windows, where an open handle blocks segment deletion.
type Reader struct {
	l *Log

	want    uint64 // don't return anything below this
	f       *os.File
	segBase uint64
	off     int64
	pos     uint64 // index of the record sitting at off

	buf    []byte
	bufOff int64

	Lost uint64 // records we never saw because retention ate them
}

func (l *Log) NewReader(from uint64) *Reader {
	return &Reader{l: l, want: from}
}

func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// Next returns the next record, or ErrNoData if we caught up with the writer.
func (r *Reader) Next() (uint64, []byte, error) {
	for {
		segs := r.l.snapshot() // before reading: if a newer segment exists, this one is final
		if r.f == nil {
			if err := r.open(segs); err != nil {
				return 0, nil, err
			}
		}

		payload, err := r.readRecord()
		switch {
		case err == nil:
			idx := r.pos
			r.pos++
			if idx < r.want {
				continue
			}
			r.want = idx + 1
			return idx, append([]byte(nil), payload...), nil

		case errors.Is(err, errShort):
			if next, ok := nextSeg(segs, r.segBase); ok {
				r.advance(next)
				continue
			}
			return 0, nil, ErrNoData

		case errors.Is(err, ErrCorrupt):
			if next, ok := nextSeg(segs, r.segBase); ok {
				r.advance(next)
				continue
			}
			return 0, nil, ErrCorrupt

		default:
			return 0, nil, err
		}
	}
}

// SkipCorrupt abandons the rest of the current segment after ErrCorrupt.
func (r *Reader) SkipCorrupt() {
	r.l.rollIfActive(r.segBase)
	segs := r.l.snapshot()
	if next, ok := nextSeg(segs, r.segBase); ok {
		r.advance(next)
	}
}

func nextSeg(segs []segment, base uint64) (segment, bool) {
	for _, s := range segs {
		if s.base > base {
			return s, true
		}
	}
	return segment{}, false
}

func (r *Reader) advance(s segment) {
	r.f.Close()
	r.f = nil
	r.segBase, r.pos, r.off = s.base, s.base, 0
	r.buf = r.buf[:0]
	if r.want < s.base {
		r.Lost += s.base - r.want
		r.want = s.base
	}
	r.openSeg(s)
}

func (r *Reader) openSeg(s segment) {
	f, err := os.Open(s.path)
	if err != nil {
		return // retried (and handled) by open() on the next Next()
	}
	r.f = f
}

func (r *Reader) open(segs []segment) error {
	if len(segs) == 0 {
		return ErrNoData
	}
	if r.want < segs[0].base {
		r.Lost += segs[0].base - r.want
		r.want = segs[0].base
	}
	i := 0
	for j, s := range segs {
		if s.base <= r.want {
			i = j
		}
	}
	s := segs[i]
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			// segment was retired between snapshot and open; go again with fresh state
			return ErrNoData
		}
		return err
	}
	r.f, r.segBase, r.pos, r.off = f, s.base, s.base, 0
	r.buf = r.buf[:0]
	return nil
}

func (r *Reader) peek(n int) ([]byte, error) {
	if r.off >= r.bufOff && r.off+int64(n) <= r.bufOff+int64(len(r.buf)) {
		s := r.off - r.bufOff
		return r.buf[s : s+int64(n)], nil
	}
	size := max(n, readChunk)
	if cap(r.buf) < size {
		r.buf = make([]byte, size)
	}
	r.buf = r.buf[:size]
	m, err := r.f.ReadAt(r.buf, r.off)
	r.buf = r.buf[:m]
	r.bufOff = r.off
	if m < n {
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		return nil, errShort
	}
	return r.buf[:n], nil
}

func (r *Reader) readRecord() ([]byte, error) {
	h, err := r.peek(headerSize)
	if err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(h))
	sum := binary.BigEndian.Uint32(h[4:])
	if n > MaxRecordSize {
		return nil, ErrCorrupt
	}
	rec, err := r.peek(headerSize + n)
	if err != nil {
		return nil, err
	}
	p := rec[headerSize:]
	if crc32.Checksum(p, castagnoli) != sum {
		return nil, ErrCorrupt
	}
	r.off += int64(headerSize + n)
	return p, nil
}
