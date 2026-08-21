// Package manifest defines the recipe that reassembles an object from its
// chunks, and the interface for wherever those recipes are kept.
package manifest

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound reports that no manifest exists for a key.
var ErrNotFound = errors.New("manifest not found")

// ChunkRef names one chunk of an object and records what it should
// contain.
type ChunkRef struct {
	ID     string `json:"id"`
	Size   int64  `json:"size"`
	CRC32C uint32 `json:"crc32c"`
}

// Manifest is an object's recipe: the ordered list of chunks that compose
// it, plus enough metadata to verify the result.
//
// Chunks is ordered and that order is the only record of how chunks
// compose into an object. Size is the exact byte count; the final chunk is
// generally shorter than ChunkSize, and reassembly truncates to Size.
type Manifest struct {
	Key       string     `json:"key"`
	Size      int64      `json:"size"`
	ChunkSize int        `json:"chunk_size"`
	Chunks    []ChunkRef `json:"chunks"`
	ETag      string     `json:"etag"`
	CreatedAt time.Time  `json:"created_at"`
}

// Store keeps manifests.
//
// In M1 the only implementation is DirStore, which writes JSON files
// beside the client. From M2 the metadata server implements this same
// interface over gRPC and Raft, and the client changes not at all.
type Store interface {
	// Put stores a manifest, replacing any manifest already under its key.
	Put(ctx context.Context, m *Manifest) error
	// Get returns the manifest for key, or ErrNotFound.
	Get(ctx context.Context, key string) (*Manifest, error)
	// Delete removes the manifest for key. Deleting an absent key
	// succeeds.
	Delete(ctx context.Context, key string) error
	// List returns every key beginning with prefix. An empty prefix
	// returns all keys.
	List(ctx context.Context, prefix string) ([]string, error)
}
