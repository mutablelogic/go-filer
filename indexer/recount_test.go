package indexer_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	fsevents "github.com/mutablelogic/go-filer/backend/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/indexer"
	store "github.com/mutablelogic/go-filer/store"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// recounts is a store which counts the batches of work which are done, and
// how often the files of the index are counted
type recounts struct {
	store.Store
	batches, counts atomic.Int64
}

func (s *recounts) Done(ctx context.Context, volume string, work ...store.Work) error {
	s.batches.Add(1)
	return s.Store.Done(ctx, volume, work...)
}

func (s *recounts) CountObjects(ctx context.Context, volume string, req store.ListRequest) (uint64, error) {
	if req.Objects {
		s.counts.Add(1)
	}
	return s.Store.CountObjects(ctx, volume, req)
}

func Test_RecountCoalesced(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, dir := newBackend(t)
	s := &recounts{Store: newStore(t)}
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	fileCount := func() uint64 {
		state, err := i.State(context.Background())
		require.NoError(t, err)
		return state.Files
	}

	// Many files, of many batches of work, are counted at most every while,
	// rather than of each batch, and once their work is done
	batches, counts := s.batches.Load(), s.counts.Load()
	for n := range 500 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d.txt", n)), []byte("f"), 0o644))
	}
	require.Eventually(t, func() bool { return fileCount() == 502 }, 20*time.Second, 10*time.Millisecond, "the files which are added")
	batches, counts = s.batches.Load()-batches, s.counts.Load()-counts
	t.Logf("%d batches, %d counts", batches, counts)
	assert.Greater(t, batches, int64(2), "of many batches")
	assert.Less(t, counts, batches, "they're counted less often than each batch")
}
