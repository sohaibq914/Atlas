package node

import (
	"context"
	"errors"
	"fmt"
	"io"

	atlasv1 "github.com/sohaibq914/atlas/gen/atlas/v1"
	"github.com/sohaibq914/atlas/internal/chunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// readFrameSize is how many bytes each ReadChunk response carries.
const readFrameSize = 64 << 10

// Server exposes a Store over gRPC as a ChunkService.
type Server struct {
	atlasv1.UnimplementedChunkServiceServer
	store *Store
}

// NewServer returns a ChunkService backed by store.
func NewServer(store *Store) *Server {
	return &Server{store: store}
}

// Register attaches this service to a gRPC server.
func (s *Server) Register(gs *grpc.Server) {
	atlasv1.RegisterChunkServiceServer(gs, s)
}

// WriteChunk receives a chunk. The first message must be the header; all
// subsequent messages carry data.
func (s *Server) WriteChunk(stream atlasv1.ChunkService_WriteChunkServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected a header message: %v", err)
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first message must carry the header")
	}
	id, err := chunk.ParseID(header.GetChunkId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid chunk id: %v", err)
	}
	if len(header.GetForwardTo()) > 0 {
		return status.Error(codes.Unimplemented, "chained replication arrives in M2")
	}

	size, crc, err := s.store.Write(id, &streamReader{stream: stream})
	if err != nil {
		return status.Errorf(codes.Internal, "store chunk: %v", err)
	}

	return stream.SendAndClose(&atlasv1.WriteChunkResponse{
		Size:     uint64(size),
		Crc32C:   crc,
		Replicas: 1,
	})
}

// ReadChunk streams a chunk back to the caller. The store verifies the
// chunk's checksum before any bytes are sent.
func (s *Server) ReadChunk(req *atlasv1.ReadChunkRequest, stream atlasv1.ChunkService_ReadChunkServer) error {
	id, err := chunk.ParseID(req.GetChunkId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid chunk id: %v", err)
	}
	data, err := s.store.Read(id)
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Errorf(codes.NotFound, "chunk %s not found", id)
	case errors.Is(err, ErrCorrupt):
		return status.Errorf(codes.DataLoss, "chunk %s failed verification: %v", id, err)
	case err != nil:
		return status.Errorf(codes.Internal, "read chunk: %v", err)
	}

	for off := 0; off < len(data); off += readFrameSize {
		end := off + readFrameSize
		if end > len(data) {
			end = len(data)
		}
		if serr := stream.Send(&atlasv1.ReadChunkResponse{Data: data[off:end]}); serr != nil {
			return fmt.Errorf("send chunk %s: %w", id, serr)
		}
	}
	return nil
}

// DeleteChunk removes a chunk. Deleting an absent chunk succeeds, because
// the control loop reissues instructions and a repeated delete must not
// fail.
func (s *Server) DeleteChunk(_ context.Context, req *atlasv1.DeleteChunkRequest) (*atlasv1.DeleteChunkResponse, error) {
	id, err := chunk.ParseID(req.GetChunkId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk id: %v", err)
	}
	if err := s.store.Delete(id); err != nil {
		return nil, status.Errorf(codes.Internal, "delete chunk: %v", err)
	}
	return &atlasv1.DeleteChunkResponse{}, nil
}

// streamReader adapts the data messages of a WriteChunk stream to an
// io.Reader, so the store can consume the chunk without the whole thing
// being buffered in memory first.
type streamReader struct {
	stream atlasv1.ChunkService_WriteChunkServer
	buf    []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		req, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, fmt.Errorf("receive chunk data: %w", err)
		}
		if req.GetHeader() != nil {
			return 0, status.Error(codes.InvalidArgument, "unexpected second header message")
		}
		r.buf = req.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
