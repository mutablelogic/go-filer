//go:build darwin && cgo

package fsevents

import (
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

func TestRel(t *testing.T) {
	for _, tc := range []struct {
		root, path, rel string
		ok              bool
	}{
		{"/Users/a", "/Users/a", "", true},
		{"/Users/a", "/Users/a/b/c.txt", "b/c.txt", true},
		{"/Users/a", "/users/A/b", "b", true},
		{"/Users/a", "/Users/ab", "", false},
		{"/Users/a", "/Users", "", false},
		{"/", "/", "", true},
		{"/", "/Users/a/b.txt", "Users/a/b.txt", true},
	} {
		rel, ok := (&watcher{root: tc.root}).rel(tc.path)
		assert.Equal(t, tc.ok, ok, "%q in %q", tc.path, tc.root)
		assert.Equal(t, tc.rel, rel, "%q in %q", tc.path, tc.root)
	}
}
