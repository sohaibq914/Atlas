// Package chunk defines chunk identity, checksums, and stream splitting.
package chunk

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// IDSize is the length of a chunk identifier in bytes.
const IDSize = 16

// ID is a random 128-bit chunk identifier.
//
// IDs are deliberately random rather than derived from chunk contents.
// Content addressing would imply deduplication, whose reference-counting
// errors cause silent data loss. See the design spec, section 2.
type ID [IDSize]byte

// NewID returns a fresh random chunk identifier.
func NewID() (ID, error) {
	var id ID
	if _, err := rand.Read(id[:]); err != nil {
		return ID{}, fmt.Errorf("generate chunk id: %w", err)
	}
	return id, nil
}

// String renders the identifier as 32 lowercase hex characters.
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// Shard returns the two-hex-character directory prefix under which the
// chunk is stored. It bounds the number of entries in any one directory.
func (id ID) Shard() string {
	return hex.EncodeToString(id[:1])
}

// ParseID decodes a chunk identifier from its hex representation.
func ParseID(s string) (ID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, fmt.Errorf("parse chunk id %q: %w", s, err)
	}
	if len(b) != IDSize {
		return ID{}, fmt.Errorf("parse chunk id %q: want %d bytes, got %d", s, IDSize, len(b))
	}
	var id ID
	copy(id[:], b)
	return id, nil
}
