// Package node implements the Atlas storage node: a local chunk store and
// the gRPC service that fronts it.
//! Create store
//! Write chunk safely
//! Read and verify chunk
//! Delete chunk
//! Check whether chunk exists
//! List all chunks


package node //! This file belongs to the node package, which implements the storage node.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sohaibq914/atlas/internal/chunk"
)

var (
	// ErrNotFound reports that a chunk is absent from this store.
	ErrNotFound = errors.New("chunk not found")
	// ErrCorrupt reports that a chunk's bytes do not match its recorded
	// size and checksum.
	ErrCorrupt = errors.New("chunk is corrupt")
)

// Store keeps immutable chunks as files under a root directory.
//
// Layout: <root>/<shard>/<id>.dat holds the bytes and <id>.meta holds the
// size and checksum. Chunks are written to a temporary name, fsynced, and
// then renamed into place, so a .dat file is never visible in a partial
// state, and never exists without its .meta.
type Store struct {
	root string //! Because root starts with a lowercase letter, only code inside the node package can access it directly.
}

// chunkMeta is the JSON payload of a .meta file.
//! Each chunk has a small metadata file containing:
//! - Its expected size
//!  - Its expected CRC32C checksum
type chunkMeta struct {
	Size   int64  `json:"size"`
	CRC32C uint32 `json:"crc32c"`
}

// NewStore opens (creating if needed) a chunk store rooted at root.
func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create store root %s: %w", root, err)
	}
	return &Store{root: root}, nil
}

// Root returns the store's root directory.
//! store knows where every chunk and metadata file belongs
func (s *Store) Root() string { return s.root }
func (s *Store) dir(id chunk.ID) string  { return filepath.Join(s.root, id.Shard()) }
func (s *Store) path(id chunk.ID) string { return filepath.Join(s.dir(id), id.String()+".dat") }
func (s *Store) meta(id chunk.ID) string { return filepath.Join(s.dir(id), id.String()+".meta") }

// Write stores the contents of r under id, returning the number of bytes
// written and their CRC32C.
//
// Chunk ids are random, so in normal operation an id is written exactly
// once. Callers must not rely on overwrite semantics: chunks are
// immutable, and rewriting an existing id is a bug in the caller.
func (s *Store) Write(id chunk.ID, r io.Reader) (int64, uint32, error) {
	dir := s.dir(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("create shard directory %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		return 0, 0, fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	// Removes the temp file on every failure path. After a successful
	// rename the path no longer exists and this is a harmless no-op.
	defer func() { _ = os.Remove(tmpName) }()

	h := chunk.NewHasher()
	size, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		_ = tmp.Close()
		return 0, 0, fmt.Errorf("write chunk %s: %w", id, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, 0, fmt.Errorf("fsync chunk %s: %w", id, err)
	}
	if err := tmp.Close(); err != nil {
		return 0, 0, fmt.Errorf("close chunk %s: %w", id, err)
	}

	crc := h.Sum32()

	// Metadata lands first so that a .dat file never exists without it.
	if err := s.writeMeta(id, chunkMeta{Size: size, CRC32C: crc}); err != nil {
		return 0, 0, err
	}
	if err := os.Rename(tmpName, s.path(id)); err != nil {
		return 0, 0, fmt.Errorf("publish chunk %s: %w", id, err)
	}
	return size, crc, nil
}

func (s *Store) writeMeta(id chunk.ID, m chunkMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode metadata for chunk %s: %w", id, err)
	}
	tmp, err := os.CreateTemp(s.dir(id), "meta-*")
	if err != nil {
		return fmt.Errorf("create temp metadata for chunk %s: %w", id, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write metadata for chunk %s: %w", id, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync metadata for chunk %s: %w", id, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close metadata for chunk %s: %w", id, err)
	}
	if err := os.Rename(tmpName, s.meta(id)); err != nil {
		return fmt.Errorf("publish metadata for chunk %s: %w", id, err)
	}
	return nil
}

func (s *Store) readMeta(id chunk.ID) (chunkMeta, error) {
	b, err := os.ReadFile(s.meta(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return chunkMeta{}, fmt.Errorf("chunk %s: %w", id, ErrNotFound)
		}
		return chunkMeta{}, fmt.Errorf("read metadata for chunk %s: %w", id, err)
	}
	var m chunkMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return chunkMeta{}, fmt.Errorf("chunk %s metadata is unreadable: %w", id, ErrCorrupt)
	}
	return m, nil
}

// Read returns a chunk's bytes, verifying its size and checksum. It
// returns ErrNotFound if the chunk is absent and ErrCorrupt if the bytes
// on disk no longer match what was recorded when they were written.
func (s *Store) Read(id chunk.ID) ([]byte, error) {
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("chunk %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("read chunk %s: %w", id, err)
	}
	m, err := s.readMeta(id)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != m.Size {
		return nil, fmt.Errorf("chunk %s is %d bytes, expected %d: %w", id, len(data), m.Size, ErrCorrupt)
	}
	if got := chunk.CRC(data); got != m.CRC32C {
		return nil, fmt.Errorf("chunk %s checksum %#x, expected %#x: %w", id, got, m.CRC32C, ErrCorrupt)
	}
	return data, nil
}

// Delete removes a chunk. Deleting an absent chunk succeeds: instructions
// are reissued by the level-triggered control loop, so a delete may arrive
// more than once and must not fail the second time.
func (s *Store) Delete(id chunk.ID) error {
	for _, p := range []string{s.path(id), s.meta(id)} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delete %s: %w", p, err)
		}
	}
	return nil
}

// Has reports whether the chunk's bytes are present. It does not verify
// them.
func (s *Store) Has(id chunk.ID) bool {
	_, err := os.Stat(s.path(id))
	return err == nil
}

// List returns the identifiers of every chunk in the store.
func (s *Store) List() ([]chunk.ID, error) {
	var ids []chunk.ID
	err := filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".dat") {
			return nil
		}
		id, perr := chunk.ParseID(strings.TrimSuffix(d.Name(), ".dat"))
		if perr != nil {
			// A file we did not write. Ignore it rather than failing the
			// whole listing.
			return nil
		}
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}
	return ids, nil
}
