package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/sohaibq914/atlas/internal/manifest"
	"github.com/sohaibq914/atlas/internal/node"
	"github.com/sohaibq914/atlas/pkg/client"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestM1EndToEnd runs a real storage node over a real TCP listener and
// round-trips a multi-chunk object through the real client, exercising the
// same code path the binaries use.
func TestM1EndToEnd(t *testing.T) {
	store, err := node.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	gs := grpclib.NewServer()
	node.NewServer(store).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpclib.NewClient(
		lis.Addr().String(),
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

	// 5 MiB across 1 MiB chunks: five chunks, the last one partial.
	const chunkSize = 1 << 20
	c := client.New(conn, manifests, chunkSize)

	data := make([]byte, 5<<20+12345)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand.Read() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	m, err := c.Put(ctx, "alice/big.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if m.Size != int64(len(data)) {
		t.Fatalf("manifest Size = %d, want %d", m.Size, len(data))
	}
	if len(m.Chunks) != 6 {
		t.Fatalf("manifest has %d chunks, want 6", len(m.Chunks))
	}

	var out bytes.Buffer
	if err := c.Get(ctx, "alice/big.bin", &out); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("round-tripped object does not match what was written")
	}

	// Every chunk should be on disk under its sharded path.
	ids, err := store.List()
	if err != nil {
		t.Fatalf("store.List() error = %v", err)
	}
	if len(ids) != len(m.Chunks) {
		t.Fatalf("store holds %d chunks, manifest names %d", len(ids), len(m.Chunks))
	}
}
