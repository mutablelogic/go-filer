package indexer_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	fsevents "github.com/mutablelogic/go-filer/backend/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/indexer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Revision(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, dir := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t), indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	assert.Zero(t, i.Revision())

	// It's revised once it's scanned, and not while nothing changes
	run(t, i)
	var revision uint64
	require.Eventually(t, func() bool {
		before := i.Revision()
		time.Sleep(100 * time.Millisecond)
		revision = i.Revision()
		return revision == before
	}, 10*time.Second, time.Millisecond)
	assert.NotZero(t, revision)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, revision, i.Revision())

	// And again once a change is indexed
	require.NoError(t, os.WriteFile(filepath.Join(dir, "w.txt"), []byte("w"), 0o644))
	require.Eventually(t, func() bool { return i.Revision() > revision }, 10*time.Second, time.Millisecond)
}
