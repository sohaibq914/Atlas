package node_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	atlasv1 "github.com/sohaibq914/atlas/gen/atlas/v1"
	"github.com/sohaibq914/atlas/internal/chunk"
	"github.com/sohaibq914/atlas/internal/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newTestServer starts a ChunkService over an in-memory connection and
// returns a client for it.
func newTestServer(t *testing.T) atlasv1.ChunkServiceClient {
	t.Helper()

	store, err := node.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	node.NewServer(store).Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return atlasv1.NewChunkServiceClient(conn)
}

// writeChunk streams data to the server under id.
func writeChunk(t *testing.T, c atlasv1.ChunkServiceClient, id chunk.ID, data []byte) *atlasv1.WriteChunkResponse {
	t.Helper()
	stream, err := c.WriteChunk(context.Background())
	if err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	err = stream.Send(&atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Header{
			Header: &atlasv1.WriteChunkHeader{ChunkId: id.String()},
		},
	})
	if err != nil {
		t.Fatalf("Send(header) error = %v", err)
	}
	for off := 0; off < len(data); off += 8 {
		end := off + 8
		if end > len(data) {
			end = len(data)
		}
		sendErr := stream.Send(&atlasv1.WriteChunkRequest{
			Payload: &atlasv1.WriteChunkRequest_Data{Data: data[off:end]},
		})
		if sendErr != nil {
			t.Fatalf("Send(data) error = %v", sendErr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv() error = %v", err)
	}
	return resp
}

// readChunk drains a ReadChunk stream into a byte slice.
func readChunk(t *testing.T, c atlasv1.ChunkServiceClient, id chunk.ID) ([]byte, error) {
	t.Helper()
	stream, err := c.ReadChunk(context.Background(), &atlasv1.ReadChunkRequest{ChunkId: id.String()})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for {
		resp, rerr := stream.Recv()
		if rerr == io.EOF {
			return buf.Bytes(), nil
		}
		if rerr != nil {
			return nil, rerr
		}
		buf.Write(resp.GetData())
	}
}

func TestServerWriteThenRead(t *testing.T) {
	c := newTestServer(t)
	id := mustID(t)
	data := bytes.Repeat([]byte("atlas"), 1000)

	resp := writeChunk(t, c, id, data)
	if resp.GetSize() != uint64(len(data)) {
		t.Fatalf("WriteChunk size = %d, want %d", resp.GetSize(), len(data))
	}
	if want := chunk.CRC(data); resp.GetCrc32C() != want {
		t.Fatalf("WriteChunk crc = %#x, want %#x", resp.GetCrc32C(), want)
	}
	if resp.GetReplicas() != 1 {
		t.Fatalf("WriteChunk replicas = %d, want 1", resp.GetReplicas())
	}

	got, err := readChunk(t, c, id)
	if err != nil {
		t.Fatalf("ReadChunk() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("ReadChunk returned %d bytes, want %d, and they differ", len(got), len(data))
	}
}

func TestServerReadMissingReturnsNotFound(t *testing.T) {
	c := newTestServer(t)
	_, err := readChunk(t, c, mustID(t))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ReadChunk() code = %v, want NotFound", status.Code(err))
	}
}

func TestServerRejectsMissingHeader(t *testing.T) {
	c := newTestServer(t)
	stream, err := c.WriteChunk(context.Background())
	if err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	sendErr := stream.Send(&atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Data{Data: []byte("no header first")},
	})
	if sendErr != nil && sendErr != io.EOF {
		t.Fatalf("Send() error = %v", sendErr)
	}
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CloseAndRecv() code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestServerRejectsBadChunkID(t *testing.T) {
	c := newTestServer(t)
	stream, err := c.WriteChunk(context.Background())
	if err != nil {
		t.Fatalf("WriteChunk() error = %v", err)
	}
	sendErr := stream.Send(&atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Header{
			Header: &atlasv1.WriteChunkHeader{ChunkId: "not-a-chunk-id"},
		},
	})
	if sendErr != nil && sendErr != io.EOF {
		t.Fatalf("Send() error = %v", sendErr)
	}
	_, err = stream.CloseAndRecv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CloseAndRecv() code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestServerDeleteIsIdempotent(t *testing.T) {
	c := newTestServer(t)
	id := mustID(t)
	writeChunk(t, c, id, []byte("delete me"))

	req := &atlasv1.DeleteChunkRequest{ChunkId: id.String()}
	if _, err := c.DeleteChunk(context.Background(), req); err != nil {
		t.Fatalf("first DeleteChunk() error = %v", err)
	}
	if _, err := c.DeleteChunk(context.Background(), req); err != nil {
		t.Fatalf("second DeleteChunk() error = %v, want nil (deletes are idempotent)", err)
	}
	if _, err := readChunk(t, c, id); status.Code(err) != codes.NotFound {
		t.Fatalf("ReadChunk() after delete code = %v, want NotFound", status.Code(err))
	}
}

func TestServerEmptyChunk(t *testing.T) {
	c := newTestServer(t)
	id := mustID(t)
	resp := writeChunk(t, c, id, nil)
	if resp.GetSize() != 0 {
		t.Fatalf("WriteChunk size = %d, want 0", resp.GetSize())
	}
	got, err := readChunk(t, c, id)
	if err != nil {
		t.Fatalf("ReadChunk() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ReadChunk returned %d bytes, want 0", len(got))
	}
}
