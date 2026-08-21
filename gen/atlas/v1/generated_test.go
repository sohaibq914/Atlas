package atlasv1_test

import (
	"testing"

	atlasv1 "github.com/sohaibq914/atlas/gen/atlas/v1"
	"google.golang.org/protobuf/proto"
)

func TestWriteChunkRequestOneof(t *testing.T) {
	header := &atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Header{
			Header: &atlasv1.WriteChunkHeader{ChunkId: "abc"},
		},
	}
	if header.GetHeader().GetChunkId() != "abc" {
		t.Fatalf("GetChunkId() = %q, want %q", header.GetHeader().GetChunkId(), "abc")
	}
	if header.GetData() != nil {
		t.Fatal("GetData() returned non-nil for a header message")
	}

	data := &atlasv1.WriteChunkRequest{
		Payload: &atlasv1.WriteChunkRequest_Data{Data: []byte("payload")},
	}
	if string(data.GetData()) != "payload" {
		t.Fatalf("GetData() = %q, want %q", data.GetData(), "payload")
	}
	if data.GetHeader() != nil {
		t.Fatal("GetHeader() returned non-nil for a data message")
	}
}

func TestChunkRefRoundTrip(t *testing.T) {
	want := &atlasv1.ChunkRef{ChunkId: "a3f9", Size: 8388608, Crc32C: 0xE3069283}
	b, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	got := &atlasv1.ChunkRef{}
	if err := proto.Unmarshal(b, got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got.GetChunkId() != want.GetChunkId() || got.GetSize() != want.GetSize() || got.GetCrc32C() != want.GetCrc32C() {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}
