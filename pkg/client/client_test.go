package client_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/sohaibq914/atlas/internal/manifest"
	"github.com/sohaibq914/atlas/internal/node"
	"github.com/sohaibq914/atlas/pkg/client"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const testChunkSize = 1024

// newClient starts an in-process storage node and returns a client wired
// to it, using a small chunk size so tests stay fast.
func newClient(t *testing.T) *client.Client {
	t.Helper()

	store, err := node.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	gs := grpclib.NewServer()
	node.NewServer(store).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpclib.NewClient(
		"passthrough:///bufnet",
		grpclib.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	manifests, err := manifest.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewDirStore() error = %v", err)
	}

	return client.New(conn, manifests, testChunkSize)
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read() error = %v", err)
	}
	return b
}

func TestPutGetRoundTrip(t *testing.T) {
	sizes := []int{
		0,
		1,
		testChunkSize - 1,
		testChunkSize,
		testChunkSize + 1,
		3 * testChunkSize,
		3*testChunkSize + 517,
	}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			c := newClient(t)
			ctx := context.Background()
			data := randomBytes(t, size)

			m, err := c.Put(ctx, "alice/blob.bin", bytes.NewReader(data))
			if err != nil {
				t.Fatalf("Put() error = %v", err)
			}
			if m.Size != int64(size) {
				t.Fatalf("manifest Size = %d, want %d", m.Size, size)
			}
			wantChunks := (size + testChunkSize - 1) / testChunkSize
			if len(m.Chunks) != wantChunks {
				t.Fatalf("manifest has %d chunks, want %d", len(m.Chunks), wantChunks)
			}

			var out bytes.Buffer
			if err := c.Get(ctx, "alice/blob.bin", &out); err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !bytes.Equal(out.Bytes(), data) {
				t.Fatalf("Get() returned %d bytes, want %d, and they differ", out.Len(), len(data))
			}
		})
	}
}

func TestPutRecordsChunkChecksums(t *testing.T) {
	c := newClient(t)
	data := randomBytes(t, 2*testChunkSize)
	m, err := c.Put(context.Background(), "k", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	var total int64
	for i, ref := range m.Chunks {
		if ref.ID == "" {
			t.Fatalf("chunk %d has an empty id", i)
		}
		if ref.CRC32C == 0 && ref.Size != 0 {
			t.Fatalf("chunk %d has a zero checksum for %d bytes", i, ref.Size)
		}
		total += ref.Size
	}
	if total != m.Size {
		t.Fatalf("chunk sizes total %d, manifest Size = %d", total, m.Size)
	}
}

func TestPutPreservesChunkOrder(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	// Distinct bytes per chunk, so any reordering shows up in the output.
	// Four full chunks plus a partial fifth.
	var data []byte
	for i := 0; i < 4; i++ {
		data = append(data, bytes.Repeat([]byte{byte('A' + i)}, testChunkSize)...)
	}
	data = append(data, bytes.Repeat([]byte("Z"), 100)...)

	m, err := c.Put(ctx, "ordered", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	var out bytes.Buffer
	if err := c.Get(ctx, "ordered", &out); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("Get() returned the chunks in the wrong order")
	}
	if m.ETag == "" {
		t.Fatal("manifest ETag is empty")
	}
}

func TestGetMissingKey(t *testing.T) {
	c := newClient(t)
	var out bytes.Buffer
	err := c.Get(context.Background(), "does-not-exist", &out)
	if !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestPutOverwriteReplacesManifest(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	first := randomBytes(t, 3*testChunkSize)
	if _, err := c.Put(ctx, "k", bytes.NewReader(first)); err != nil {
		t.Fatalf("first Put() error = %v", err)
	}
	second := randomBytes(t, testChunkSize/2)
	if _, err := c.Put(ctx, "k", bytes.NewReader(second)); err != nil {
		t.Fatalf("second Put() error = %v", err)
	}

	var out bytes.Buffer
	if err := c.Get(ctx, "k", &out); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(out.Bytes(), second) {
		t.Fatal("Get() returned the first version after an overwrite")
	}
}

func TestDeleteRemovesKey(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	var out bytes.Buffer
	if err := c.Get(ctx, "k", &out); !errors.Is(err, manifest.ErrNotFound) {
		t.Fatalf("Get() after Delete() error = %v, want ErrNotFound", err)
	}
}

func TestList(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	for _, k := range []string{"alice/1", "alice/2", "bob/1"} {
		if _, err := c.Put(ctx, k, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put(%q) error = %v", k, err)
		}
	}
	got, err := c.List(ctx, "alice/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List(\"alice/\") returned %v, want 2 keys", got)
	}
}
