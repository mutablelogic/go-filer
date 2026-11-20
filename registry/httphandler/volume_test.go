package httphandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	google "github.com/mutablelogic/go-filer/backend/google"
	registry "github.com/mutablelogic/go-filer/registry"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestVolumeList(t *testing.T) {
	server, dir := newServer(t)

	// The volume, with the URL of its directory, and the schemes
	var list schema.VolumeList
	require.Equal(t, http.StatusOK, get(t, server, "volume", &list))
	if assert.Len(t, list.Body, 1) {
		assert.Equal(t, "media", list.Body[0].Name)
		u, err := url.Parse(list.Body[0].URL)
		require.NoError(t, err)
		assert.Equal(t, "file", u.Scheme)
		assert.Equal(t, "media", u.Host)
		assert.Equal(t, filepath.Base(dir), filepath.Base(filepath.FromSlash(u.Path)))
	}
	assert.Equal(t, []schema.Backend{
		{Scheme: "file", Description: "Folders of the server"},
		{Scheme: "gcalendar", Description: "Google Calendar events of a Google account"},
		{Scheme: "gcontacts", Description: "Google Contacts of a Google account"},
		{Scheme: "gtasks", Description: "Google Tasks of a Google account"},
		{Scheme: "s3", Description: "S3 buckets, or those which are compatible, such as MinIO and SeaweedFS"},
	}, list.Backends)
}

// post requests the path with the body as JSON, and decodes the response
// into v when it's 201
func post(t *testing.T, server *httptest.Server, path string, body, v any) int {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(server.URL+"/api/"+path, "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

func TestVolumeCreate(t *testing.T) {
	// The directory of the volume is in the server's, so it's removed after
	// the registry is closed, as on Windows, a directory which is open can't
	// be removed
	server, base := newServer(t)
	dir, err := os.MkdirTemp(base, "music-")
	require.NoError(t, err)
	fileURL := func(name, path, query string) string {
		return (&url.URL{Scheme: "file", Host: name, Path: filepath.ToSlash(path), RawQuery: query}).String()
	}

	// A volume is created, with the URL of its backend, which has its
	// options, and is listed
	var volume schema.Volume
	require.Equal(t, http.StatusCreated, post(t, server, "volume", schema.VolumeCreateRequest{Name: "music", URL: fileURL("music", dir, "readonly=true&package=")}, &volume))
	assert.Equal(t, "music", volume.Name)
	u, err := url.Parse(volume.URL)
	require.NoError(t, err)
	assert.Equal(t, "true", u.Query().Get("readonly"))
	assert.True(t, u.Query().Has("package"))
	var list schema.VolumeList
	require.Equal(t, http.StatusOK, get(t, server, "volume", &list))
	if assert.Len(t, list.Body, 2) {
		assert.Equal(t, volume.VolumeMeta, list.Body[1].VolumeMeta)
	}

	// Its objects can be listed, once it's mounted
	require.NoError(t, os.WriteFile(filepath.Join(dir, "song.mp3"), nil, 0o644))
	var objects schema.ObjectList
	require.Eventually(t, func() bool { return get(t, server, "object/music", &objects) == http.StatusOK }, 10*time.Second, 5*time.Millisecond, "the volume isn't mounted")
	if assert.Len(t, objects.Body, 1) {
		assert.Equal(t, "/song.mp3", objects.Body[0].Path)
	}

	// Errors
	for name, test := range map[string]struct {
		body any
		want int
	}{
		"Exists":        {schema.Volume{Name: "music", URL: fileURL("music", dir, "")}, http.StatusConflict},
		"NoURL":         {schema.VolumeCreateRequest{Name: "other"}, http.StatusBadRequest},
		"BadURL":        {schema.VolumeCreateRequest{Name: "other", URL: "file://other/%zz"}, http.StatusBadRequest},
		"OtherName":     {schema.VolumeCreateRequest{Name: "other", URL: fileURL("music", dir, "")}, http.StatusBadRequest},
		"BadName":       {schema.Volume{Name: "not a name", URL: fileURL("not a name", dir, "")}, http.StatusBadRequest},
		"Scheme":        {schema.VolumeCreateRequest{Name: "other", URL: "ftp://other/data"}, http.StatusBadRequest},
		"NoPath":        {schema.VolumeCreateRequest{Name: "other", URL: "file://other"}, http.StatusBadRequest},
		"Missing":       {schema.VolumeCreateRequest{Name: "other", URL: fileURL("other", filepath.Join(dir, "missing"), "")}, http.StatusBadRequest},
		"UnknownOption": {schema.VolumeCreateRequest{Name: "other", URL: fileURL("other", dir, "unknown=1")}, http.StatusBadRequest},
		"BadOption":     {schema.VolumeCreateRequest{Name: "other", URL: fileURL("other", dir, "readonly=maybe")}, http.StatusBadRequest},
		"Body":          {"not a volume", http.StatusBadRequest},
	} {
		assert.Equal(t, test.want, post(t, server, "volume", test.body, nil), name)
	}
}

// del requests the path with DELETE, and decodes the response into v when
// it's 200
func del(t *testing.T, server *httptest.Server, path string, v any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, server.URL+"/api/"+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

func TestVolumeGet(t *testing.T) {
	server, _ := newServer(t)

	// The volume, with the URL of its backend, which is the one listed
	var volume schema.Volume
	require.Equal(t, http.StatusOK, get(t, server, "volume/media", &volume))
	var list schema.VolumeList
	require.Equal(t, http.StatusOK, get(t, server, "volume", &list))
	if assert.Len(t, list.Body, 1) {
		assert.Equal(t, volume.VolumeMeta, list.Body[0].VolumeMeta)
	}

	// A volume which isn't mounted, or was deleted, isn't found
	assert.Equal(t, http.StatusNotFound, get(t, server, "volume/missing", nil))
	require.Equal(t, http.StatusOK, del(t, server, "volume/media", nil))
	assert.Equal(t, http.StatusNotFound, get(t, server, "volume/media", nil))
}

func TestVolumeDelete(t *testing.T) {
	server, dir := newServer(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), nil, 0o644))

	// The volume is deleted, and returned, with the URL of its backend, and
	// isn't listed, nor are its objects, but its files aren't deleted
	var volume schema.Volume
	require.Equal(t, http.StatusOK, del(t, server, "volume/media", &volume))
	assert.Equal(t, "media", volume.Name)
	assert.Contains(t, volume.URL, "file://media/")
	var list schema.VolumeList
	require.Equal(t, http.StatusOK, get(t, server, "volume", &list))
	assert.Empty(t, list.Body)
	require.Eventually(t, func() bool { return get(t, server, "object/media", nil) == http.StatusNotFound }, 10*time.Second, 5*time.Millisecond, "its objects aren't listed, once it's unmounted")
	_, err := os.Stat(filepath.Join(dir, "a.txt"))
	assert.NoError(t, err)

	// It can be created again, from its URL
	require.Equal(t, http.StatusCreated, post(t, server, "volume", volume, nil))

	// A volume which isn't mounted isn't found
	assert.Equal(t, http.StatusNotFound, del(t, server, "volume/missing", nil))
}

// patch requests the path with PATCH and the body, and decodes the response
// into v when it's 200
func patch(t *testing.T, server *httptest.Server, path, body string, v any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPatch, server.URL+"/api/"+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

func TestVolumeUpdate(t *testing.T) {
	server, _ := newServer(t)
	options := func(volume schema.Volume) url.Values {
		u, err := url.Parse(volume.URL)
		require.NoError(t, err)
		return u.Query()
	}

	// The volume's URL has each of its options, which are the defaults, as
	// none are set
	var volume schema.Volume
	require.Equal(t, http.StatusOK, get(t, server, "volume/media", &volume))
	defaults := options(volume)
	assert.Equal(t, "false", defaults.Get("readonly"))
	with := func(key, value string) url.Values {
		result := url.Values{}
		for option, values := range defaults {
			result[option] = values
		}
		result.Set("readonly", "true")
		result.Set(key, value)
		return result
	}

	// Options are set, and kept, and removed with null, so they're the
	// defaults
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"readonly": true, "skip": ["\\.tmp$"]}`, &volume))
	assert.Equal(t, with("skip", `\.tmp$`), options(volume))
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"skip": null}`, &volume))
	assert.Equal(t, with("readonly", "true"), options(volume))
	require.Equal(t, http.StatusOK, get(t, server, "volume/media", &volume))
	assert.Equal(t, with("readonly", "true"), options(volume))

	// Without options, it's opened again, as it is
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{}`, &volume))
	assert.Equal(t, with("readonly", "true"), options(volume))

	// Errors
	for body, want := range map[string]int{
		`{"unknown": true}`:    http.StatusBadRequest,
		`{"readonly": "what"}`: http.StatusBadRequest,
		`{"readonly": {}}`:     http.StatusBadRequest,
		`not json`:             http.StatusBadRequest,
	} {
		assert.Equal(t, want, patch(t, server, "volume/media", body, nil), body)
	}
	assert.Equal(t, http.StatusNotFound, patch(t, server, "volume/missing", `{"readonly": true}`, nil))
	assert.Equal(t, http.StatusBadRequest, patch(t, server, "volume/media?enabled=maybe", `{}`, nil))
	require.Equal(t, http.StatusOK, get(t, server, "volume/media", &volume))
	assert.Equal(t, with("readonly", "true"), options(volume))

	// It's disabled, which unmounts it, keeping its options, and enabled
	// again, which mounts it
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media?enabled=false", `{}`, &volume))
	assert.False(t, volume.Enabled)
	assert.Equal(t, with("readonly", "true"), options(volume))
	require.Eventually(t, func() bool { return get(t, server, "object/media", nil) == http.StatusServiceUnavailable }, 10*time.Second, 5*time.Millisecond, "it's unmounted")
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"skip": null}`, &volume))
	assert.False(t, volume.Enabled, "it's kept disabled, when it's updated")
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media?enabled=true", `{}`, &volume))
	assert.True(t, volume.Enabled)
	require.Eventually(t, func() bool { return get(t, server, "object/media", nil) == http.StatusOK }, 10*time.Second, 5*time.Millisecond, "it's mounted")
}

// credentials is a provider of credentials, in memory
type credentials map[string]json.RawMessage

func (c credentials) Expand(_ context.Context, value string) (string, error) { return value, nil }
func (c credentials) Set(_ context.Context, key string, value json.RawMessage) error {
	c[key] = value
	return nil
}

func (c credentials) Delete(_ context.Context, key string) error {
	delete(c, key)
	return nil
}
func (c credentials) Get(_ context.Context, key string) (json.RawMessage, error) {
	if value, exists := c[key]; exists {
		return value, nil
	}
	return nil, auth.ErrNotFound.Withf("credential %q", key)
}

func TestVolumeCreateSignIn(t *testing.T) {
	// A volume of Google Tasks, whose user hasn't signed in, is 401, with
	// how they sign in, without the client's secret
	server, _ := newServer(t, registry.WithCredentials(credentials{
		google.ClientCredential: json.RawMessage(`{"installed":{"client_id":"client-1","client_secret":"client-secret"}}`),
	}))
	data, err := json.Marshal(schema.VolumeCreateRequest{Name: "tasks", URL: "gtasks://tasks"})
	require.NoError(t, err)
	resp, err := http.Post(server.URL+"/api/volume", "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	var body struct {
		Detail authclient.SignIn `json:"detail"`
	}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &body))
	assert.Equal(t, google.Google, body.Detail.Server)
	assert.Equal(t, "client-1", body.Detail.ClientID)
	assert.Equal(t, google.DefaultCredential, body.Detail.Credential)
	assert.Equal(t, []string{"https://www.googleapis.com/auth/tasks.readonly", "https://www.googleapis.com/auth/userinfo.email"}, body.Detail.Scopes)
	assert.NotContains(t, string(raw), "client-secret")

	// A volume of Google Tasks with a path isn't one
	assert.Equal(t, http.StatusBadRequest, post(t, server, "volume", schema.VolumeCreateRequest{Name: "tasks", URL: "gtasks://tasks/lists"}, nil))
}
