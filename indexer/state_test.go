package indexer

import (
	"context"
	"errors"
	"sync"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_updateState(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.New(ctx, gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp?index=objects", Enabled: true})
	require.NoError(t, err)
	self := &Indexer{name: "media", store: &revised{Store: s}}
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady}))

	// Changes of its fields, at once, such as of its files, of a recount,
	// and of why it failed, of its watch, aren't written over of the others
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			_ = self.updateState(ctx, func(state *schema.IndexState) bool {
				state.Objects++
				return true
			})
		})
		wg.Go(func() { self.failed(ctx, errors.New("failed "+string(rune('a'+i%26)))) })
	}
	wg.Wait()
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), state.Objects, "each of its changes")
	assert.NotEmpty(t, state.Error)
	assert.Equal(t, schema.IndexStatusReady, state.Status)

	// A change which doesn't change it isn't written
	revision := self.Revision()
	require.NoError(t, self.updateState(ctx, func(*schema.IndexState) bool { return false }))
	assert.Equal(t, revision, self.Revision())
}
