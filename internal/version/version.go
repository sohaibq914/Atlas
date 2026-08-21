// Package version exposes the build version stamped into Atlas binaries.
package version

// value is overridden at build time with:
//
//	go build -ldflags "-X github.com/sohaibq914/atlas/internal/version.value=v0.1.0"
var value = "dev"

// Version returns the build version, or "dev" for unstamped builds.
func Version() string {
	if value == "" {
		return "dev"
	}
	return value
}
