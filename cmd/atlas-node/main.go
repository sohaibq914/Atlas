// Command atlas-node runs an Atlas storage node: a local chunk store
// fronted by the ChunkService gRPC API.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sohaibq914/atlas/internal/node"
	"github.com/sohaibq914/atlas/internal/version"
	"google.golang.org/grpc"
)

func main() {
	var (
		addr        = flag.String("addr", ":9001", "address to listen on")
		dataDir     = flag.String("data-dir", "./data/node1", "directory holding this node's chunks")
		showVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Version())
		return
	}

	if err := run(*addr, *dataDir); err != nil {
		log.Fatalf("atlas-node: %v", err)
	}
}

func run(addr, dataDir string) error {
	store, err := node.NewStore(dataDir)
	if err != nil {
		return fmt.Errorf("open chunk store: %w", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	gs := grpc.NewServer()
	node.NewServer(store).Register(gs)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("atlas-node: shutting down")
		gs.GracefulStop()
	}()

	log.Printf("atlas-node %s listening on %s, data in %s", version.Version(), addr, store.Root())
	if err := gs.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
