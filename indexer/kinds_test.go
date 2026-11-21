package indexer_test

import (
	"context"
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

// kindReads is a store which counts how often its kinds are read
type kindReads struct {
	store.Store
	reads atomic.Int64
}

func (s *kindReads) Kinds(ctx context.Context, volume string) (map[string]uint64, error) {
	s.reads.Add(1)
	return s.Store.Kinds(ctx, volume)
}

func Test_Kinds(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, dir := newBackend(t)
	s := &kindReads{Store: newStore(t)}
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	ctx := context.Background()

	// Its objects' kinds, of their types, which are read once, while it isn't
	// revised
	var kinds map[string]uint64
	require.Eventually(t, func() bool {
		kinds, err = i.Kinds(ctx)
		return err == nil && kinds["text/plain"] == 2
	}, 10*time.Second, 10*time.Millisecond, "its two text files")
	// Once it's settled, as it can be revised a while after, such as of a
	// change which inotify sends late, which is counted again
	require.Eventually(t, func() bool {
		revision := i.Revision()
		time.Sleep(100 * time.Millisecond)
		return i.Revision() == revision
	}, 10*time.Second, time.Millisecond, "it settles")
	kinds, err = i.Kinds(ctx)
	require.NoError(t, err)
	reads, revision := s.reads.Load(), i.Revision()
	for range 5 {
		again, err := i.Kinds(ctx)
		require.NoError(t, err)
		assert.Equal(t, kinds, again)
	}
	require.Equal(t, revision, i.Revision(), "it isn't revised meanwhile")
	assert.Equal(t, reads, s.reads.Load(), "they're read once, of a revision")

	// And again, once it's revised
	require.NoError(t, os.WriteFile(filepath.Join(dir, "z.txt"), []byte("z"), 0o644))
	require.Eventually(t, func() bool {
		kinds, err = i.Kinds(ctx)
		return err == nil && kinds["text/plain"] == 3
	}, 10*time.Second, 10*time.Millisecond, "a file which is added")
	assert.Greater(t, s.reads.Load(), reads)
}

func Test_KindsFolders(t *testing.T) {
	backend, _ := newBackend(t)
	s := &kindReads{Store: newStore(t)}

	// Of an index of folders, its objects aren't indexed, so it has no kinds
	i, err := indexer.New("media", backend, s, indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	kinds, err := i.Kinds(context.Background())
	require.NoError(t, err)
	assert.Nil(t, kinds)
	assert.Zero(t, s.reads.Load())
}

func Test_KindsRefresh(t *testing.T) {
	backend, _ := newBackend(t)
	s := &kindReads{Store: newStore(t)}
	t.Cleanup(indexer.SetKindsRefresh(50 * time.Millisecond))

	// Kinds of a revision are counted again once a while has passed, as
	// another process, which doesn't revise it, can change them
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	_, err = i.Kinds(context.Background())
	require.NoError(t, err)
	_, err = i.Kinds(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), s.reads.Load(), "of a revision")
	time.Sleep(60 * time.Millisecond)
	_, err = i.Kinds(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), s.reads.Load(), "once a while has passed")
}
