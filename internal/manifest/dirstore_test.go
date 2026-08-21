package manifest_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/sohaibq914/atlas/internal/manifest"
)

func newDirStore(t *testing.T) *manifest.DirStore {
	t.Helper()
	s, err := manifest.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewDirStore() error = %v", err)
	}
	return s
}

func sample(key string) *manifest.Manifest {
	return &manifest.Manifest{
		Key:       key,
		Size:      24117248,
		ChunkSize: 8 << 20,
		Chunks: []manifest.ChunkRef{
			{ID: "a3f9e2b1c4d5060708090a0b0c0d0e0f", Size: 8 << 20, CRC32C: 0x11111111},
			{ID: "7c2d1f0498ab060708090a0b0c0d0e0f", Size: 8 << 20, CRC32C: 0x22222222},
			{ID: "0102030405060708090a0b0c0d0e0f10", Size: 7340032, CRC32C: 0x33333333},
		},
		ETag:      "deadbeef",
		CreatedAt: time.Date(2026, 8, 21, 14, 3, 0, 0, time.UTC),
	}
}

func TestDirStoreRoundTrip(t *testing.T) {
	s := newDirStore(t)
	ctx := context.Background()
	want := sample("alice/report.pdf")

	if err := s.Put(ctx, want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := s.Get(ctx, want.Key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Key != want.Key || got.Size != want.Size || got.ChunkSize != want.ChunkSize {
		t.Fatalf("Get() = %+v, want %+v", got, want)
	}
	if got.ETag != want.ETag {
		t.Fatalf("ETag = %q, want %q", got.ETag, want.ETag)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	if len(got.Chunks) != len(want.Chunks) {
		t.Fatalf("got %d chunks, want %d", len(got.Chunks), len(want.Chunks))
	}
	for i := range want.Chunks {
		if got.Chunks[i] != want.Chunks[i] {
			t.Fatalf("chunk %d = %+v, want %+v", i, got.Chunks[i], want.Chunks[i])
		}
	}
}

func TestDirStorePreservesChunkOrder(t *testing.T) {
	s := newDirStore(t)
	ctx := context.Background()
	m := sample("ordered")
	if err := s.Put(ctx, m); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := s.Get(ctx, "ordered")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	for i := range m.Chunks {
		if got.Chunks[i].ID != m.Chunks[i].ID {
			t.Fatalf("chunk order changed at index %d: %s != %s", i, got.Chunks[i].ID, m.Chunks[i].ID)
		}
	}
}

func TestDirStoreHandlesKeysWithSlashes(t *testing.T) {
	s := newDirStore(t)
	ctx := context.Background()
	keys := []string{
		"alice/report.pdf",
		"a/b/c/d/deeply/nested.bin",
		"../escape-attempt",
		"with spaces and #hash?query",
	}
	for _, k := range keys {
		if err := s.Put(ctx, sample(k)); err != nil {
			t.Fatalf("Put(%q) error = %v", k, err)
		}
		got, err := s.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", k, err)
		}
		if got.Key != k {
			t.Fatalf("Get(%q).Key = %q", k, got.Key)
		}
	}
}

func TestDirStoreGetMissingReturnsErrNotFound(t *testing.T) {
	s := newDirStore(t)
	_, err := s.Get(context.Background(), "nope")
	if !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestDirStoreDeleteIsIdempotent(t *testing.T) {
	s := newDirStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, sample("gone")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := s.Delete(ctx, "gone"); err != nil {
		t.Fatalf("first Delete() error = %v", err)
	}
	if err := s.Delete(ctx, "gone"); err != nil {
		t.Fatalf("second Delete() error = %v, want nil", err)
	}
	if _, err := s.Get(ctx, "gone"); !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("Get() after delete error = %v, want ErrNotFound", err)
	}
}

func TestDirStoreListByPrefix(t *testing.T) {
	s := newDirStore(t)
	ctx := context.Background()
	for _, k := range []string{"alice/a.txt", "alice/b.txt", "bob/c.txt"} {
		if err := s.Put(ctx, sample(k)); err != nil {
			t.Fatalf("Put(%q) error = %v", k, err)
		}
	}

	got, err := s.List(ctx, "alice/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	sort.Strings(got)
	want := []string{"alice/a.txt", "alice/b.txt"}
	if len(got) != len(want) {
		t.Fatalf("List(\"alice/\") = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List(\"alice/\") = %v, want %v", got, want)
		}
	}

	all, err := s.List(ctx, "")
	if err != nil {
		t.Fatalf("List(\"\") error = %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("List(\"\") returned %d keys, want 3", len(all))
	}
}
