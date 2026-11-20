package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsbackend "github.com/mutablelogic/go-filer/backend/fs"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// actioned is a backend of a directory, with actions: touch creates a file,
// of its name, echo returns an object of its name, without creating it, and
// nothing returns no object
type actioned struct {
	example
}

type testAction struct {
	name string
	run  func(context.Context, json.RawMessage) (*schema.Object, error)
}

type nameInput struct {
	Name string `json:"name"`
}

func (a testAction) Name() string              { return a.name }
func (a testAction) Title() string             { return strings.ToUpper(a.name) }
func (a testAction) Description() string       { return "The " + a.name + " action" }
func (a testAction) Input() *jsonschema.Schema { return jsonschema.MustFor[nameInput]() }
func (a testAction) Run(ctx context.Context, input json.RawMessage) (*schema.Object, error) {
	return a.run(ctx, input)
}

func (b actioned) Actions() []gofiler.Action {
	name := func(input json.RawMessage) (string, error) {
		var in nameInput
		if err := json.Unmarshal(input, &in); err != nil || in.Name == "" {
			return "", gofiler.ErrBadParameter.With("a name is needed")
		}
		return in.Name, nil
	}
	return []gofiler.Action{
		testAction{"touch", func(ctx context.Context, input json.RawMessage) (*schema.Object, error) {
			n, err := name(input)
			if err != nil {
				return nil, err
			}
			return b.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/" + n}, Body: strings.NewReader(n)})
		}},
		testAction{"echo", func(ctx context.Context, input json.RawMessage) (*schema.Object, error) {
			n, err := name(input)
			if err != nil {
				return nil, err
			}
			return &schema.Object{ObjectKey: schema.ObjectKey{Path: "/" + n}}, nil
		}},
		testAction{"nothing", func(context.Context, json.RawMessage) (*schema.Object, error) {
			return nil, nil
		}},
	}
}

func Test_Registry_Actions(t *testing.T) {
	dir := t.TempDir()
	open := func(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
		b, err := fsbackend.New(VolumeName(u), dir, nil, opts)
		if err != nil {
			return nil, err
		}
		return actioned{example{Filer: b, u: u}}, nil
	}
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(newStore(t)), WithBackend("actions", "Actions", open))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	_, err = add(t, r, "things", mustURL(t, "actions://things"))
	require.NoError(t, err)
	_, err = add(t, r, "media", fileURL(t, t.TempDir(), "media"))
	require.NoError(t, err)
	ctx := context.Background()

	// A volume's actions are listed, in order of their names, and a
	// filesystem's are none
	list, err := r.ListActions(ctx, "things")
	require.NoError(t, err)
	assert.Equal(t, "things", list.Name)
	names := []string{}
	for _, action := range list.Body {
		names = append(names, action.Name)
	}
	assert.Equal(t, []string{"echo", "nothing", "touch"}, names)
	assert.Equal(t, "TOUCH", list.Body[2].Title)
	assert.Equal(t, "The touch action", list.Body[2].Description)
	assert.NotNil(t, list.Body[2].Input)
	list, err = r.ListActions(ctx, "media")
	require.NoError(t, err)
	assert.Empty(t, list.Body)

	// An action is called, of its name, with its input, and returns its
	// object, of the volume
	object, err := r.CallAction(ctx, "things", "touch", json.RawMessage(`{"name":"a.txt"}`))
	require.NoError(t, err)
	assert.Equal(t, "things", object.Name)
	assert.Equal(t, "/a.txt", object.Path)
	data, err := os.ReadFile(filepath.Join(dir, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "a.txt", string(data))

	// Its errors are its own, an action which isn't the backend's isn't
	// found, nor is a volume which doesn't exist, and an action which returns
	// no object is an error
	_, err = r.CallAction(ctx, "things", "touch", json.RawMessage(`{}`))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = r.CallAction(ctx, "things", "missing", json.RawMessage(`{}`))
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.CallAction(ctx, "media", "touch", json.RawMessage(`{"name":"b.txt"}`))
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.CallAction(ctx, "nowhere", "touch", json.RawMessage(`{"name":"b.txt"}`))
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.ListActions(ctx, "nowhere")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.CallAction(ctx, "things", "nothing", nil)
	assert.ErrorIs(t, err, gofiler.ErrInternalServerError)

	// A volume which is offline, as its watch failed, has none available
	r.RLock()
	e := r.backend["things"]
	r.RUnlock()
	r.setOffline(e, gofiler.ErrServiceUnavailable.With("the watch failed"))
	_, err = r.ListActions(ctx, "things")
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	assert.ErrorContains(t, err, "offline")
	_, err = r.CallAction(ctx, "things", "echo", json.RawMessage(`{"name":"a"}`))
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	r.setOffline(e, nil)
	_, err = r.CallAction(ctx, "things", "echo", json.RawMessage(`{"name":"a"}`))
	require.NoError(t, err, "nor once it's online again")

	// Nor does a volume which is disabled
	_, err = r.UpdateVolume(ctx, schema.VolumeMeta{Name: "things", URL: "actions://things", Enabled: false})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := r.ListActions(ctx, "things")
		return errors.Is(err, gofiler.ErrServiceUnavailable) && strings.Contains(err.Error(), "unmounted")
	}, 5*time.Second, time.Millisecond)
	_, err = r.CallAction(ctx, "things", "echo", json.RawMessage(`{"name":"a"}`))
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
}
