package chunk_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/sohaibq914/atlas/internal/chunk"
)

// collect runs Split over in and returns the size of each chunk produced,
// along with the concatenation of all chunks.
func collect(t *testing.T, in []byte, size int) ([]int, []byte) {
	t.Helper()
	var sizes []int
	var joined bytes.Buffer
	err := chunk.Split(bytes.NewReader(in), size, func(data []byte) error {
		sizes = append(sizes, len(data))
		joined.Write(data)
		return nil
	})
	if err != nil {
		t.Fatalf("Split() error = %v", err)
	}
	return sizes, joined.Bytes()
}

func TestSplitChunkSizes(t *testing.T) {
	cases := []struct {
		name  string
		input int
		size  int
		want  []int
	}{
		{"empty input produces no chunks", 0, 4, nil},
		{"one byte", 1, 4, []int{1}},
		{"exactly one chunk", 4, 4, []int{4}},
		{"one chunk plus a byte", 5, 4, []int{4, 1}},
		{"exactly two chunks", 8, 4, []int{4, 4}},
		{"partial final chunk", 9, 4, []int{4, 4, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := bytes.Repeat([]byte("x"), tc.input)
			sizes, joined := collect(t, in, tc.size)
			if len(sizes) != len(tc.want) {
				t.Fatalf("got %d chunks %v, want %d %v", len(sizes), sizes, len(tc.want), tc.want)
			}
			for i := range sizes {
				if sizes[i] != tc.want[i] {
					t.Fatalf("chunk sizes = %v, want %v", sizes, tc.want)
				}
			}
			if !bytes.Equal(joined, in) {
				t.Fatal("concatenated chunks do not equal the input")
			}
		})
	}
}

func TestSplitPreservesContent(t *testing.T) {
	in := []byte("abcdefghijklmnopqrstuvwxyz")
	var got [][]byte
	err := chunk.Split(bytes.NewReader(in), 7, func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		got = append(got, cp)
		return nil
	})
	if err != nil {
		t.Fatalf("Split() error = %v", err)
	}
	want := []string{"abcdefg", "hijklmn", "opqrstu", "vwxyz"}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("chunk %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSplitRejectsNonPositiveSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		err := chunk.Split(bytes.NewReader([]byte("data")), size, func([]byte) error {
			return nil
		})
		if err == nil {
			t.Fatalf("Split(size=%d) succeeded, want an error", size)
		}
	}
}

func TestSplitPropagatesCallbackError(t *testing.T) {
	sentinel := errors.New("callback failed")
	calls := 0
	err := chunk.Split(bytes.NewReader(bytes.Repeat([]byte("x"), 100)), 4, func([]byte) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Split() error = %v, want %v", err, sentinel)
	}
	if calls != 1 {
		t.Fatalf("callback invoked %d times, want 1 (should stop on error)", calls)
	}
}

func TestSplitPropagatesReaderError(t *testing.T) {
	r := &failingReader{after: 6, err: errors.New("disk exploded")}
	err := chunk.Split(r, 4, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("Split() succeeded, want the reader error")
	}
	if !strings.Contains(err.Error(), "disk exploded") {
		t.Fatalf("Split() error = %v, want it to mention the reader error", err)
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
