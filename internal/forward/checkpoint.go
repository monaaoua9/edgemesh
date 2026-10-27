package forward

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Checkpoint remembers the next WAL index that still needs to go upstream.
// A lost or corrupt file just means we replay, which the cloud dedupes.
type Checkpoint struct {
	path string
}

func OpenCheckpoint(dir string) *Checkpoint {
	return &Checkpoint{path: filepath.Join(dir, "checkpoint")}
}

func (c *Checkpoint) Load() uint64 {
	b, err := os.ReadFile(c.path)
	if err != nil || len(b) != 12 {
		return 0
	}
	if crc32.ChecksumIEEE(b[:8]) != binary.BigEndian.Uint32(b[8:]) {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (c *Checkpoint) Store(next uint64) error {
	var b [12]byte
	binary.BigEndian.PutUint64(b[:], next)
	binary.BigEndian.PutUint32(b[8:], crc32.ChecksumIEEE(b[:8]))

	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
