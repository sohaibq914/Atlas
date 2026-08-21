package node_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sohaibq914/atlas/internal/chunk"
	"github.com/sohaibq914/atlas/internal/node"
)

func newStore(t *testing.T) (*node.Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := node.NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return s, root
}

func mustID(t *testing.T) chunk.ID {
	t.Helper()
	id, err := chunk.NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	return id
}

func TestStoreWriteThenRead(t *testing.T) {
	s, _ := newStore(t)
	id := mustID(t)
	data := []byte("the bytes of a chunk")

	size, crc, err := s.Write(id, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("Write() size = %d, want %d", size, len(data))
	}
	if want := chunk.CRC(data); crc != want {
		t.Fatalf("Write() crc = %#x, want %#x", crc, want)
	}

	got, err := s.Read(id)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Read() = %q, want %q", got, data)
	}
}

func TestStoreWriteUsesShardedPath(t *testing.T) {
	s, root := newStore(t)
	id := mustID(t)
	if _, _, err := s.Write(id, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	want := filepath.Join(root, id.Shard(), id.String()+".dat")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected chunk at %s: %v", want, err)
	}
	meta := filepath.Join(root, id.Shard(), id.String()+".meta")
	if _, err := os.Stat(meta); err != nil {
		t.Fatalf("expected metadata at %s: %v", meta, err)
	}
}

func TestStoreReadMissingReturnsErrNotFound(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Read(mustID(t))
	if !errors.Is(err, node.ErrNotFound) {
		t.Fatalf("Read() error = %v, want ErrNotFound", err)
	}
}

func TestStoreReadDetectsCorruption(t *testing.T) {
	s, root := newStore(t)
	id := mustID(t)
	if _, _, err := s.Write(id, bytes.NewReader([]byte("original contents"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Flip a bit on disk, simulating rot.
	p := filepath.Join(root, id.Shard(), id.String()+".dat")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	raw[0] ^= 0x01
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = s.Read(id)
	if !errors.Is(err, node.ErrCorrupt) {
		t.Fatalf("Read() error = %v, want ErrCorrupt", err)
	}
}

func TestStoreReadDetectsTruncation(t *testing.T) {
	s, root := newStore(t)
	id := mustID(t)
	if _, _, err := s.Write(id, bytes.NewReader([]byte("original contents"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	p := filepath.Join(root, id.Shard(), id.String()+".dat")
	if err := os.Truncate(p, 4); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	_, err := s.Read(id)
	if !errors.Is(err, node.ErrCorrupt) {
		t.Fatalf("Read() error = %v, want ErrCorrupt", err)
	}
}

func TestStoreDeleteIsIdempotent(t *testing.T) {
	s, _ := newStore(t)
	id := mustID(t)
	if _, _, err := s.Write(id, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("first Delete() error = %v", err)
	}
	if s.Has(id) {
		t.Fatal("Has() = true after Delete()")
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("second Delete() error = %v, want nil (deletes are idempotent)", err)
	}
}

func TestStoreList(t *testing.T) {
	s, _ := newStore(t)
	want := map[chunk.ID]bool{}
	for i := 0; i < 5; i++ {
		id := mustID(t)
		if _, _, err := s.Write(id, bytes.NewReader([]byte("data"))); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		want[id] = true
	}
	got, err := s.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("List() returned %d ids, want %d", len(got), len(want))
	}
	for _, id := range got {
		if !want[id] {
			t.Fatalf("List() returned unexpected id %s", id)
		}
	}
}

func TestStoreFailedWriteLeavesNoChunk(t *testing.T) {
	s, root := newStore(t)
	id := mustID(t)
	r := &failingReader{after: 3, err: errors.New("stream died")}

	if _, _, err := s.Write(id, r); err == nil {
		t.Fatal("Write() succeeded, want the reader error")
	}
	if s.Has(id) {
		t.Fatal("Has() = true after a failed write; a partial chunk was published")
	}

	// No temporary files should be left behind either.
	entries, err := os.ReadDir(filepath.Join(root, id.Shard()))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("shard directory contains %d leftover files, want 0", len(entries))
	}
}

// failingReader yields `after` bytes and then fails.
type failingReader struct {
	after int
	err   error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.after <= 0 {
		return 0, r.err
	}
	n := len(p)
	if n > r.after {
		n = r.after
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.after -= n
	return n, nil
}
