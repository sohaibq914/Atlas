# Atlas

A fault-tolerant distributed object store in Go.

Objects are split into fixed-size chunks and replicated across storage
nodes. A Raft-replicated metadata plane holds the catalog mapping keys to
chunks and chunks to nodes. Node failure is detected by heartbeat and
replica count is restored automatically.

**Status: M1.** A single storage node, chunked writes and verified reads.
Replication, the metadata plane, and Raft arrive in M2 and M3.

## Build

Requires Go 1.27. Protocol buffer regeneration additionally requires
`protoc`; generated code is committed, so a plain build does not need it.

```bash
make build     # bin/atlas-node, bin/atlasctl
make test      # unit and integration tests, race detector on
make lint      # gofmt and go vet
make proto     # regenerate from proto/ (needs protoc and make tools)
```

## Try it

```bash
./bin/atlas-node --addr 127.0.0.1:9001 --data-dir /tmp/atlas/node1 &

dd if=/dev/urandom of=/tmp/input.bin bs=1m count=40
./bin/atlasctl put /tmp/input.bin alice/big.bin
./bin/atlasctl stat alice/big.bin
./bin/atlasctl get alice/big.bin /tmp/output.bin
shasum -a 256 /tmp/input.bin /tmp/output.bin
```

The two digests match. `/tmp/atlas/node1` holds the chunks, sharded by the
first two hex characters of each chunk id:

```
/tmp/atlas/node1/
  17/176adf4001f781adebb77a68f2265eca.dat    8 MiB of the object
  17/176adf4001f781adebb77a68f2265eca.meta   its size and CRC32C
  a3/a38eb21cdc4a32b4543f4f4ae24119e8.dat
  ...
```

Nothing in that directory records which object those chunks belong to.
That mapping lives in the manifest, which in M1 is a JSON file the client
keeps and from M2 is the metadata cluster's replicated catalog.

## Architecture

Two planes. The control plane decides where bytes go; the data plane moves
them. Object bytes never pass through the metadata servers.

| Binary | Role |
|---|---|
| `atlas-node` | Stores immutable chunks on local disk, serves `ChunkService` |
| `atlasctl` | Operator CLI: put, get, ls, rm, stat |
| `atlas-meta` | Metadata server (M2+) |
| `atlas-bench` | Load generator (M8) |

Chunks are immutable. Overwriting an object writes new chunks and swaps
its manifest atomically; the old chunks are reclaimed by garbage
collection. This removes the need to reconcile replica versions during
repair, which is the source of the nastiest bugs in this kind of system.

Every chunk carries a CRC32C checksum, verified on every read. Chunk
writes go to a temporary file, are fsynced, and are then renamed into
place, so a chunk is never visible in a partial state.

## Performance

Not yet measured. Throughput and latency figures will be published here
once the benchmark exists in M8, and they will be measurements rather than
targets.
