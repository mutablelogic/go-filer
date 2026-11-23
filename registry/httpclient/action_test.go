package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsbackend "github.com/mutablelogic/go-filer/backend/fs"
	registry "github.com/mutablelogic/go-filer/registry"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	httphandler "github.com/mutablelogic/go-filer/registry/httphandler"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// notes is a backend of a directory, of notes://<name>, with an action,
// note, which writes a note, of its name
type notes struct {
	gofiler.Filer
	u *url.URL
}

type noteAction struct{ notes }

type noteInput struct {
	Name string `json:"name" help:"Name of the note"`
}

func (n notes) URL() *url.URL             { u := *n.u; return &u }
func (n notes) Actions() []gofiler.Action { return []gofiler.Action{noteAction{n}} }
func (noteAction) Name() string           { return "note" }
func (noteAction) Title() string          { return "Write a note" }
func (noteAction) Description() string    { return "Writes a note, of its name" }
func (noteAction) Input() *jsonschema.Schema {
	return jsonschema.MustFor[noteInput]()
}
func (a noteAction) Run(ctx context.Context, input json.RawMessage) (*schema.Object, error) {
	var in noteInput
	if err := json.Unmarshal(input, &in); err != nil || in.Name == "" {
		return nil, gofiler.ErrBadParameter.With("a note has a name")
	}
	return a.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/" + in.Name}, Body: strings.NewReader(in.Name)})
}

func TestActions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.New(ctx, gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	open := func(ctx context.Context, u *url.URL, _ gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		b, err := fsbackend.New(registry.VolumeName(u), dir, nil, opts)
		if err != nil {
			return nil, err
		}
		return notes{Filer: b, u: u}, nil
	}
	reg, err := registry.New(ctx, registry.WithDebounce(0), registry.WithStore(s), registry.WithBackend(testBackend("notes", "Notes", open)))
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	running := make(chan error, 1)
	go func() { running <- reg.Run(runCtx, nil) }()
	require.Eventually(t, reg.Running, 5*time.Second, time.Millisecond)
	t.Cleanup(func() { cancel(); <-running })
	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(ctx, mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(reg, router))

	// Calling an action declares its input, as an optional body of JSON
	spec, err := json.Marshal(router.Spec())
	require.NoError(t, err)
	var doc struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(spec, &doc))
	var body bool
	for path, item := range doc.Paths {
		if !strings.HasSuffix(path, "/action/{name}/{action}") {
			continue
		}
		var post struct {
			RequestBody *struct {
				Required bool                       `json:"required"`
				Content  map[string]json.RawMessage `json:"content"`
			} `json:"requestBody"`
		}
		require.NoError(t, json.Unmarshal(item["post"], &post))
		require.NotNil(t, post.RequestBody, path)
		assert.False(t, post.RequestBody.Required)
		assert.Contains(t, post.RequestBody.Content, "application/json")
		body = true
	}
	assert.True(t, body, "it's in the spec")
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client, err := httpclient.New(server.URL + "/api")
	require.NoError(t, err)

	_, err = client.CreateVolume(ctx, schema.VolumeCreateRequest{Name: "notes", URL: "notes://notes"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := client.ListActions(ctx, "notes")
		return err == nil
	}, 5*time.Second, time.Millisecond, "it's mounted")

	// Its actions are listed, with their inputs
	list, err := client.ListActions(ctx, "notes")
	require.NoError(t, err)
	require.Len(t, list.Body, 1)
	assert.Equal(t, "notes", list.Body[0].Volume)
	assert.Equal(t, "note", list.Body[0].Name)
	assert.Equal(t, "Write a note", list.Body[0].Title)
	require.NotNil(t, list.Body[0].Input)

	// And every volume's, of no volume
	list, err = client.ListActions(ctx, "")
	require.NoError(t, err)
	require.Len(t, list.Body, 1)
	assert.Equal(t, "notes", list.Body[0].Volume)

	// An action is called, and returns its object, of the volume
	object, err := client.CallAction(ctx, "notes", "note", json.RawMessage(`{"name":"a.txt"}`))
	require.NoError(t, err)
	assert.Equal(t, "notes", object.Name)
	assert.Equal(t, "/a.txt", object.Path)
	data, err := os.ReadFile(filepath.Join(dir, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "a.txt", string(data))

	// The action's error, an action, or volume, which doesn't exist, and
	// input which isn't JSON
	_, err = client.CallAction(ctx, "notes", "note", nil)
	assert.Equal(t, http.StatusBadRequest, status(err), err)
	_, err = client.CallAction(ctx, "notes", "missing", nil)
	assert.Equal(t, http.StatusNotFound, status(err), err)
	_, err = client.ListActions(ctx, "nowhere")
	assert.Equal(t, http.StatusNotFound, status(err), err)
	resp, err := http.Post(server.URL+"/api/action/notes/note", "application/json", strings.NewReader("not json"))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// A volume which is unmounted has none available
	_, err = client.UpdateVolume(ctx, "notes", nil, func() *bool { b := false; return &b }())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := client.ListActions(ctx, "notes")
		return status(err) == http.StatusServiceUnavailable
	}, 5*time.Second, time.Millisecond)
	list, err = client.ListActions(ctx, "")
	require.NoError(t, err)
	assert.Empty(t, list.Body, "nor are they listed, of every volume")
}

// funcBackend is a backend of a test, of its scheme, what it is, and the
// function which opens a volume of it
type funcBackend struct {
	scheme, description string
	open                func(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error)
}

// testBackend returns the backend of the scheme, of what it is, and the
// function which opens a volume of it, or nil, without one
func testBackend(scheme, description string, open func(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error)) gofiler.Backend {
	if open == nil {
		return nil
	}
	return funcBackend{scheme, description, open}
}

func (b funcBackend) Scheme() string       { return b.scheme }
func (b funcBackend) Description() string  { return b.description }
func (b funcBackend) Icon() gofiler.Icon   { return nil }
func (b funcBackend) Defaults() url.Values { return url.Values{} }
func (b funcBackend) New(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	return b.open(ctx, u, cache, opts)
}
