package chunk

import (
	"hash"
	"hash/crc32"
)

// castagnoli is the CRC32C table. Modern CPUs implement this polynomial as
// a hardware instruction, so checksumming runs at multiple GB/s.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC returns the CRC32C checksum of b.
func CRC(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// NewHasher returns a streaming CRC32C hasher, for checksumming bytes as
// they pass through without buffering them.
func NewHasher() hash.Hash32 {
	return crc32.New(castagnoli)
}
