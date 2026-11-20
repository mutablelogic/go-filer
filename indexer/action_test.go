package indexer_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// actioned is a backend which isn't watched, with actions which write a
// file, and delete one, of the backend, which the index isn't told of
type actioned struct {
	*unwatched
}

type fileAction struct {
	name string
	run  func(context.Context) (*schema.Object, error)
}

func (a fileAction) Name() string              { return a.name }
func (a fileAction) Title() string             { return a.name }
func (a fileAction) Description() string       { return a.name }
func (a fileAction) Input() *jsonschema.Schema { return nil }
func (a fileAction) Run(ctx context.Context, _ json.RawMessage) (*schema.Object, error) {
	return a.run(ctx)
}

func (b actioned) Actions() []gofiler.Action {
	return []gofiler.Action{
		fileAction{"write", func(ctx context.Context) (*schema.Object, error) {
			return b.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/made.txt"}, Body: strings.NewReader("made")})
		}},
		fileAction{"remove", func(ctx context.Context) (*schema.Object, error) {
			deleted, err := b.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}})
			if err != nil {
				return nil, err
			}
			return deleted.Body[0], nil
		}},
	}
}

// The index of what an action creates, or deletes, is queued, once it's
// done, rather than waiting for the backend's next scan
func Test_ActionIndexed(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)
	i, err := indexer.New("media", actioned{&unwatched{Filer: backend}}, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	ctx := context.Background()
	_, err = s.GetObject(ctx, "media", "/x.txt")
	require.NoError(t, err)

	actions := i.Actions()
	require.Len(t, actions, 2)
	assert.Equal(t, "write", actions[0].Name())
	_, err = actions[0].Run(ctx, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := s.GetObject(ctx, "media", "/made.txt")
		return err == nil
	}, 5*time.Second, 5*time.Millisecond, "what it writes is indexed")
	_, err = actions[1].Run(ctx, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := s.GetObject(ctx, "media", "/x.txt")
		return err != nil
	}, 5*time.Second, 5*time.Millisecond, "what it deletes is removed")

	// A backend without actions has none
	plain, err := indexer.New("plain", &unwatched{Filer: backend}, newStore(t), indexer.WithMode(indexer.ModeObjects))
	require.NoError(t, err)
	assert.Nil(t, plain.Actions())
}
