package manifest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DirStore keeps manifests as JSON files in a single directory.
//
// This is M1 scaffolding. It exists so the client has somewhere to record
// what it wrote before the metadata server exists. M2 replaces it with a
// Store backed by the metadata cluster.
type DirStore struct {
	root string
}

var _ Store = (*DirStore)(nil)

// NewDirStore opens (creating if needed) a manifest directory at root.
func NewDirStore(root string) (*DirStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create manifest directory %s: %w", root, err)
	}
	return &DirStore{root: root}, nil
}

// path maps a key to a filename. Keys are arbitrary strings that routinely
// contain slashes, so they are base64url-encoded rather than used as paths
// directly. That keeps every manifest in one flat directory and makes
// traversal via a crafted key impossible.
func (s *DirStore) path(key string) string {
	name := base64.RawURLEncoding.EncodeToString([]byte(key))
	return filepath.Join(s.root, name+".json")
}

// Put stores a manifest, replacing any manifest already under its key.
func (s *DirStore) Put(_ context.Context, m *Manifest) error {
	if m.Key == "" {
		return errors.New("manifest key must not be empty")
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest for %q: %w", m.Key, err)
	}
	tmp, err := os.CreateTemp(s.root, "tmp-*")
	if err != nil {
		return fmt.Errorf("create temp manifest for %q: %w", m.Key, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write manifest for %q: %w", m.Key, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close manifest for %q: %w", m.Key, err)
	}
	if err := os.Rename(tmpName, s.path(m.Key)); err != nil {
		return fmt.Errorf("publish manifest for %q: %w", m.Key, err)
	}
	return nil
}

// Get returns the manifest for key, or ErrNotFound.
func (s *DirStore) Get(_ context.Context, key string) (*Manifest, error) {
	b, err := os.ReadFile(s.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("key %q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("read manifest for %q: %w", key, err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode manifest for %q: %w", key, err)
	}
	return &m, nil
}

// Delete removes the manifest for key. Deleting an absent key succeeds.
func (s *DirStore) Delete(_ context.Context, key string) error {
	if err := os.Remove(s.path(key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete manifest for %q: %w", key, err)
	}
	return nil
}

// List returns every key beginning with prefix.
func (s *DirStore) List(_ context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("list manifests: %w", err)
	}
	var keys []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, derr := base64.RawURLEncoding.DecodeString(strings.TrimSuffix(e.Name(), ".json"))
		if derr != nil {
			continue // not a file we wrote
		}
		key := string(raw)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}
