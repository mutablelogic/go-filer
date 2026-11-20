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
	store, err := sqlite.New(ctx, gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	open := func(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
		b, err := fsbackend.New(registry.VolumeName(u), dir, nil, opts)
		if err != nil {
			return nil, err
		}
		return notes{Filer: b, u: u}, nil
	}
	reg, err := registry.New(ctx, registry.WithDebounce(0), registry.WithStore(store), registry.WithBackend("notes", "Notes", open))
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
	assert.Equal(t, "notes", list.Name)
	require.Len(t, list.Body, 1)
	assert.Equal(t, "note", list.Body[0].Name)
	assert.Equal(t, "Write a note", list.Body[0].Title)
	require.NotNil(t, list.Body[0].Input)

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
	resp, err := http.Post(server.URL+"/api/volume/notes/action/note", "application/json", strings.NewReader("not json"))
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
}
