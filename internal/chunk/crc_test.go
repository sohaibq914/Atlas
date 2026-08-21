package chunk_test

import (
	"testing"

	"github.com/sohaibq914/atlas/internal/chunk"
)

// 0xE3069283 is the standard CRC32C check value for the ASCII string
// "123456789". If this fails, the wrong polynomial is in use.
func TestCRCKnownVector(t *testing.T) {
	const want = uint32(0xE3069283)
	if got := chunk.CRC([]byte("123456789")); got != want {
		t.Fatalf("CRC(\"123456789\") = %#x, want %#x", got, want)
	}
}

func TestCRCEmptyIsZero(t *testing.T) {
	if got := chunk.CRC(nil); got != 0 {
		t.Fatalf("CRC(nil) = %#x, want 0", got)
	}
}

func TestCRCDetectsSingleBitFlip(t *testing.T) {
	a := []byte("the quick brown fox")
	b := []byte("the quick brown fox")
	b[0] ^= 0x01
	if chunk.CRC(a) == chunk.CRC(b) {
		t.Fatal("CRC collided on a single bit flip")
	}
}

func TestHasherMatchesCRC(t *testing.T) {
	data := []byte("streamed in several writes")
	h := chunk.NewHasher()
	if _, err := h.Write(data[:10]); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := h.Write(data[10:]); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got, want := h.Sum32(), chunk.CRC(data); got != want {
		t.Fatalf("streaming CRC = %#x, want %#x", got, want)
	}
}
