// Package client is the Atlas client library: it splits objects into
// chunks on write and reassembles them on read.
package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	atlasv1 "github.com/sohaibq914/atlas/gen/atlas/v1"
	"github.com/sohaibq914/atlas/internal/chunk"
	"github.com/sohaibq914/atlas/internal/manifest"
	"google.golang.org/grpc"
)

// writeFrameSize is how many bytes each WriteChunk request carries.
const writeFrameSize = 64 << 10

// Client reads and writes objects.
//
// In M1 it talks to exactly one storage node and keeps manifests wherever
// the supplied manifest.Store keeps them. From M2 the node address comes
// from the metadata server per chunk, and the manifest store is the
// metadata server itself.
type Client struct {
	chunks    atlasv1.ChunkServiceClient
	manifests manifest.Store
	chunkSize int
}

// New returns a client that stores chunks on the node behind conn and
// manifests in manifests. A chunkSize of zero selects chunk.DefaultSize.
func New(conn grpc.ClientConnInterface, manifests manifest.Store, chunkSize int) *Client {
	if chunkSize <= 0 {
		chunkSize = chunk.DefaultSize
	}
	return &Client{
		chunks:    atlasv1.NewChunkServiceClient(conn),
		manifests: manifests,
		chunkSize: chunkSize,
	}
}

// Put splits r into chunks, stores each one, and records the resulting
// manifest under key. An existing object at key is replaced; its old
// chunks are left on disk for garbage collection, which arrives in M6.
func (c *Client) Put(ctx context.Context, key string, r io.Reader) (*manifest.Manifest, error) {
	if key == "" {
		return nil, fmt.Errorf("key must not be empty")
	}

	digest := sha256.New()
	var (
		refs  []manifest.ChunkRef
		total int64
	)

	err := chunk.Split(io.TeeReader(r, digest), c.chunkSize, func(data []byte) error {
		id, idErr := chunk.NewID()
		if idErr != nil {
			return idErr
		}
		size, crc, wErr := c.writeChunk(ctx, id, data)
		if wErr != nil {
			return wErr
		}
		if crc != chunk.CRC(data) {
			return fmt.Errorf("chunk %s: node reported checksum %#x, expected %#x", id, crc, chunk.CRC(data))
		}
		refs = append(refs, manifest.ChunkRef{ID: id.String(), Size: size, CRC32C: crc})
		total += size
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}

	m := &manifest.Manifest{
		Key:       key,
		Size:      total,
		ChunkSize: c.chunkSize,
		Chunks:    refs,
		ETag:      hex.EncodeToString(digest.Sum(nil)),
		CreatedAt: time.Now().UTC(),
	}
	if err := c.manifests.Put(ctx, m); err != nil {
		return nil, fmt.Errorf("put %q: %w", key, err)
	}
	return m, nil
}

// writeChunk streams one chunk to the storage node.
func (c *Client) writeChunk(ctx context.Context, id chunk.ID, data []byte) (int64, uint32, error) {
	stream, err := c.chunks.WriteChunk(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("open write stream for chunk %s: %w", id, err)
	}
	header := &atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Header{
			Header: &atlasv1.WriteChunkHeader{ChunkId: id.String()},
		},
	}
	if err := stream.Send(header); err != nil {
		return 0, 0, fmt.Errorf("send header for chunk %s: %w", id, err)
	}
	for off := 0; off < len(data); off += writeFrameSize {
		end := off + writeFrameSize
		if end > len(data) {
			end = len(data)
		}
		frame := &atlasv1.WriteChunkRequest{
			Payload: &atlasv1.WriteChunkRequest_Data{Data: data[off:end]},
		}
		if err := stream.Send(frame); err != nil {
			return 0, 0, fmt.Errorf("send data for chunk %s: %w", id, err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, 0, fmt.Errorf("finish chunk %s: %w", id, err)
	}
	return int64(resp.GetSize()), resp.GetCrc32C(), nil
}

// Get reassembles the object stored under key and writes it to w. Every
// chunk's checksum is verified before its bytes are emitted.
func (c *Client) Get(ctx context.Context, key string, w io.Writer) error {
	m, err := c.manifests.Get(ctx, key)
	if err != nil {
		return err
	}
	var written int64
	for i, ref := range m.Chunks {
		data, rErr := c.readChunk(ctx, ref)
		if rErr != nil {
			return fmt.Errorf("get %q chunk %d: %w", key, i, rErr)
		}
		n, wErr := w.Write(data)
		if wErr != nil {
			return fmt.Errorf("get %q: write output: %w", key, wErr)
		}
		written += int64(n)
	}
	if written != m.Size {
		return fmt.Errorf("get %q: reassembled %d bytes, manifest says %d", key, written, m.Size)
	}
	return nil
}

// readChunk fetches one chunk and verifies it against its manifest entry.
func (c *Client) readChunk(ctx context.Context, ref manifest.ChunkRef) ([]byte, error) {
	stream, err := c.chunks.ReadChunk(ctx, &atlasv1.ReadChunkRequest{ChunkId: ref.ID})
	if err != nil {
		return nil, fmt.Errorf("open read stream for chunk %s: %w", ref.ID, err)
	}
	data := make([]byte, 0, ref.Size)
	for {
		resp, rErr := stream.Recv()
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return nil, fmt.Errorf("receive chunk %s: %w", ref.ID, rErr)
		}
		data = append(data, resp.GetData()...)
	}
	if int64(len(data)) != ref.Size {
		return nil, fmt.Errorf("chunk %s is %d bytes, manifest says %d", ref.ID, len(data), ref.Size)
	}
	if got := chunk.CRC(data); got != ref.CRC32C {
		return nil, fmt.Errorf("chunk %s checksum %#x, manifest says %#x", ref.ID, got, ref.CRC32C)
	}
	return data, nil
}

// Stat returns the manifest for key without fetching any chunks.
func (c *Client) Stat(ctx context.Context, key string) (*manifest.Manifest, error) {
	return c.manifests.Get(ctx, key)
}

// Delete removes the object stored under key. Its chunks are left for
// garbage collection, which arrives in M6.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.manifests.Delete(ctx, key)
}

// List returns every key beginning with prefix.
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	return c.manifests.List(ctx, prefix)
}
