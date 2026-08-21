package chunk

import (
	"errors"
	"fmt"
	"io"
)

// DefaultSize is the default chunk size: 8 MiB.
const DefaultSize = 8 << 20

// Split reads r to EOF, invoking fn once per chunk. Every chunk is exactly
// size bytes except possibly the last, which may be shorter. An empty
// reader produces no calls at all.
//
// The slice passed to fn is reused between calls. Callers that need to
// retain the bytes must copy them.
func Split(r io.Reader, size int, fn func(data []byte) error) error {
	if size <= 0 {
		return fmt.Errorf("chunk size must be positive, got %d", size)
	}
	buf := make([]byte, size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if ferr := fn(buf[:n]); ferr != nil {
				return ferr
			}
		}
		switch {
		case err == nil:
			continue
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return nil
		default:
			return fmt.Errorf("read input: %w", err)
		}
	}
}
