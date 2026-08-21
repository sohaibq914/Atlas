package chunk_test

import (
	"strings"
	"testing"

	"github.com/sohaibq914/atlas/internal/chunk"
)

func TestNewIDIsThirtyTwoHexChars(t *testing.T) {
	id, err := chunk.NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	s := id.String()
	if len(s) != 32 {
		t.Fatalf("String() = %q, want 32 characters, got %d", s, len(s))
	}
	if s != strings.ToLower(s) {
		t.Fatalf("String() = %q, want lowercase", s)
	}
}

func TestNewIDIsUnique(t *testing.T) {
	seen := make(map[chunk.ID]bool, 1000)
	for i := 0; i < 1000; i++ {
		id, err := chunk.NewID()
		if err != nil {
			t.Fatalf("NewID() error = %v", err)
		}
		if seen[id] {
			t.Fatalf("NewID() produced a duplicate on iteration %d", i)
		}
		seen[id] = true
	}
}

func TestParseIDRoundTrip(t *testing.T) {
	id, err := chunk.NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	got, err := chunk.ParseID(id.String())
	if err != nil {
		t.Fatalf("ParseID(%q) error = %v", id.String(), err)
	}
	if got != id {
		t.Fatalf("ParseID round trip = %v, want %v", got, id)
	}
}

func TestParseIDRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"empty":      "",
		"too short":  "abcd",
		"too long":   strings.Repeat("ab", 17),
		"not hex":    strings.Repeat("zz", 16),
		"odd length": strings.Repeat("a", 31),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := chunk.ParseID(in); err == nil {
				t.Fatalf("ParseID(%q) succeeded, want an error", in)
			}
		})
	}
}

func TestShardIsFirstTwoHexChars(t *testing.T) {
	id, err := chunk.ParseID("a3f9e2b1c4d5060708090a0b0c0d0e0f")
	if err != nil {
		t.Fatalf("ParseID() error = %v", err)
	}
	if got := id.Shard(); got != "a3" {
		t.Fatalf("Shard() = %q, want %q", got, "a3")
	}
}
