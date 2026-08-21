// Command atlasctl is the Atlas operator CLI.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sohaibq914/atlas/internal/manifest"
	"github.com/sohaibq914/atlas/internal/version"
	"github.com/sohaibq914/atlas/pkg/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	nodeAddr    string
	manifestDir string
	chunkSize   int
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "atlasctl:", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "atlasctl",
		Short:         "Operate an Atlas object store",
		Version:       version.Version(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&nodeAddr, "node", "127.0.0.1:9001", "storage node address")
	root.PersistentFlags().StringVar(&manifestDir, "manifest-dir", "./.atlas/manifests", "where manifests are kept (M1 only)")
	root.PersistentFlags().IntVar(&chunkSize, "chunk-size", 0, "chunk size in bytes (0 selects the 8 MiB default)")

	root.AddCommand(putCmd(), getCmd(), lsCmd(), rmCmd(), statCmd())
	return root
}

// connect builds a client from the global flags. The caller must call the
// returned cleanup function.
func connect() (*client.Client, func(), error) {
	conn, err := grpc.NewClient(nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to %s: %w", nodeAddr, err)
	}
	manifests, err := manifest.NewDirStore(manifestDir)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return client.New(conn, manifests, chunkSize), func() { _ = conn.Close() }, nil
}

func putCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "put <file> <key>",
		Short: "Store a local file under a key",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, key := args[0], args[1]
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open %s: %w", path, err)
			}
			defer func() { _ = f.Close() }()

			c, cleanup, err := connect()
			if err != nil {
				return err
			}
			defer cleanup()

			m, err := c.Put(context.Background(), key, f)
			if err != nil {
				return err
			}
			cmd.Printf("%s  %d bytes  %d chunks  etag %s\n", m.Key, m.Size, len(m.Chunks), m.ETag[:12])
			return nil
		},
	}
}

func getCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key> <file>",
		Short: "Fetch an object into a local file",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, path := args[0], args[1]
			c, cleanup, err := connect()
			if err != nil {
				return err
			}
			defer cleanup()

			f, err := os.Create(path)
			if err != nil {
				return fmt.Errorf("create %s: %w", path, err)
			}
			defer func() { _ = f.Close() }()

			if err := c.Get(context.Background(), key, f); err != nil {
				return err
			}
			cmd.Printf("wrote %s\n", path)
			return nil
		},
	}
}

func lsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls [prefix]",
		Short: "List keys, optionally filtered by prefix",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			prefix := ""
			if len(args) == 1 {
				prefix = args[0]
			}
			c, cleanup, err := connect()
			if err != nil {
				return err
			}
			defer cleanup()

			keys, err := c.List(context.Background(), prefix)
			if err != nil {
				return err
			}
			for _, k := range keys {
				cmd.Println(k)
			}
			return nil
		},
	}
}

func rmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <key>",
		Short: "Delete an object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, cleanup, err := connect()
			if err != nil {
				return err
			}
			defer cleanup()
			if err := c.Delete(context.Background(), args[0]); err != nil {
				return err
			}
			cmd.Printf("deleted %s\n", args[0])
			return nil
		},
	}
}

func statCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stat <key>",
		Short: "Show an object's manifest",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, cleanup, err := connect()
			if err != nil {
				return err
			}
			defer cleanup()

			m, err := c.Stat(context.Background(), args[0])
			if err != nil {
				return err
			}
			cmd.Printf("key:        %s\n", m.Key)
			cmd.Printf("size:       %d bytes\n", m.Size)
			cmd.Printf("chunk size: %d bytes\n", m.ChunkSize)
			cmd.Printf("etag:       %s\n", m.ETag)
			cmd.Printf("created:    %s\n", m.CreatedAt.Format("2006-01-02T15:04:05Z"))
			cmd.Printf("chunks:     %d\n", len(m.Chunks))
			for i, ref := range m.Chunks {
				cmd.Printf("  [%d] %s  %d bytes  crc %#08x\n", i, ref.ID, ref.Size, ref.CRC32C)
			}
			return nil
		},
	}
}
