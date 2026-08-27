package chunk // ! every go file starts with this
// ! it says which package the files belongs to

import (
	"hash"
	"hash/crc32"
)

// castagnoli is the CRC32C table. Modern CPUs implement this polynomial as
// a hardware instruction, so checksumming runs at multiple GB/s.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// CRC returns the CRC32C checksum of b.
// ! It calculates the CRC32C checksum of those bytes and returns it as a uint32:
func CRC(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// ! this only returns the checksum calculator
// ! .Write on the return function will calculate the checksum of the bytes written to it
// ! this is useful for calculating the checksum of a stream of bytes, rather than a single byte slice
func NewHasher() hash.Hash32 {
	return crc32.New(castagnoli)
}
