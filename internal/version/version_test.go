package version_test

import (
	"testing"

	"github.com/sohaibq914/atlas/internal/version"
)

func TestVersionDefaultsToDev(t *testing.T) {
	if got := version.Version(); got != "dev" {
		t.Fatalf("Version() = %q, want %q", got, "dev")
	}
}

func TestVersionIsNeverEmpty(t *testing.T) {
	if version.Version() == "" {
		t.Fatal("Version() returned an empty string")
	}
}
