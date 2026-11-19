package google_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	credential "github.com/mutablelogic/go-auth/credential"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	oauth2 "golang.org/x/oauth2"
)

////////////////////////////////////////////////////////////////////////////////
// A FAKE OF THE TASKS API

// fakeTasks is the Tasks API, of its lists and their tasks, whose requests
// need a token, and which pages what's listed
type fakeTasks struct {
	sync.Mutex
	lists     []httpclient.TaskList
	tasks     map[string][]httpclient.Task // By their lists' ids
	requests  int
	refreshes int                          // Of the requests to the token endpoint
	queries   []url.Values                 // Of the requests for tasks
	deletes   []string                     // The paths which are deleted
	writes    []string                     // The methods, and paths, which are written
	failMove  bool                         // Moving a task fails, as Google can
	email     string                       // The user who signed in, of the userinfo endpoint, or empty, when it's forbidden
	fields    []map[string]json.RawMessage // The fields which are written
}

const (
	token    = "secret"
	clientID = "client-1"
)

// credentials is a provider of credentials, in memory
type credentials struct {
	sync.Mutex
	values map[string]json.RawMessage
}

func (c *credentials) Expand(_ context.Context, value string) (string, error) { return value, nil }

func (c *credentials) Get(_ context.Context, key string) (json.RawMessage, error) {
	c.Lock()
	defer c.Unlock()
	if value, exists := c.values[key]; exists {
		return value, nil
	}
	return nil, auth.ErrNotFound.Withf("credential %q", key)
}

func (c *credentials) Set(_ context.Context, key string, value json.RawMessage) error {
	c.Lock()
	defer c.Unlock()
	c.values[key] = value
	return nil
}

func (c *credentials) Delete(_ context.Context, key string) error {
	c.Lock()
	defer c.Unlock()
	delete(c.values, key)
	return nil
}

// credential returns the sign-in which is stored, of the key
func (c *credentials) credential(t *testing.T, key string) authclient.Grant {
	t.Helper()
	c.Lock()
	defer c.Unlock()
	var result authclient.Grant
	require.NoError(t, json.Unmarshal(c.values[key], &result))
	return result
}

// newCredentials returns the provider of the client, as the file Google's
// console downloads, and of a sign-in, whose token the fake accepts, which
// fn can change, such as to expire it
func newCredentials(t *testing.T, endpoint string, fn func(*authclient.Grant)) *credentials {
	t.Helper()
	sign := authclient.Grant{
		Server: authclient.ServerMetadata{Issuer: google.Google.Issuer, TokenEndpoint: endpoint + "/token"},
		Client: authclient.Registration{ClientID: clientID},
		Scopes: []string{httpclient.TasksScope},
		Token:  &oauth2.Token{AccessToken: token, TokenType: "Bearer", RefreshToken: "refresh", Expiry: time.Now().Add(time.Hour)},
	}
	if fn != nil {
		fn(&sign)
	}
	data, err := json.Marshal(sign)
	require.NoError(t, err)
	return &credentials{values: map[string]json.RawMessage{
		google.ClientCredential:  json.RawMessage(`{"installed":{"client_id":"` + clientID + `","client_secret":"client-secret"}}`),
		google.DefaultCredential: data,
	}}
}

func newFake(t *testing.T) (*fakeTasks, string) {
	t.Helper()
	fake := &fakeTasks{tasks: make(map[string][]httpclient.Task)}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	google.SetEndpoint(t, server.URL)
	return fake, server.URL
}

func (f *fakeTasks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.Lock()
	defer f.Unlock()
	f.requests++

	// The token endpoint refreshes a sign-in, of its refresh token, which
	// is revoked, unless it's "refresh"
	if r.URL.Path == "/token" && r.FormValue("client_secret") == "wrong-secret" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client"})
		return
	}
	if r.URL.Path == "/token" {
		f.refreshes++
		if r.FormValue("grant_type") != "refresh_token" || r.FormValue("refresh_token") != "refresh" || r.FormValue("client_id") != clientID || r.FormValue("client_secret") != "client-secret" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		writeJSON(w, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 3600})
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.Header().Set("WWW-Authenticate", `Bearer realm="https://accounts.google.com/", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	// The user who signed in, which is forbidden without the email scope
	if r.URL.Path == "/userinfo" {
		if f.email == "" {
			writeError(w, http.StatusForbidden)
		} else {
			writeJSON(w, map[string]string{"email": f.email})
		}
		return
	}

	// An id which isn't base64url, as Google's are, is a bad request, as
	// Google says it's an invalid id, rather than that it isn't found
	for _, part := range parts {
		if strings.ContainsAny(part, "~.!*") {
			writeError(w, http.StatusBadRequest)
			return
		}
	}
	switch r.Method {
	case http.MethodDelete:
		f.delete(w, parts)
		return
	case http.MethodPost, http.MethodPatch:
		f.write(w, r, parts)
		return
	}
	switch {
	case len(parts) == 3 && parts[0] == "users" && parts[2] == "lists":
		writePage(w, r, f.lists)
	case len(parts) == 4 && parts[0] == "users" && parts[2] == "lists":
		if list := f.list(parts[3]); list != nil {
			writeJSON(w, list)
		} else {
			writeError(w, http.StatusNotFound)
		}
	case len(parts) == 3 && parts[0] == "lists" && parts[2] == "tasks":
		if f.list(parts[1]) == nil {
			writeError(w, http.StatusNotFound)
			return
		}
		f.queries = append(f.queries, r.URL.Query())
		writePage(w, r, f.tasks[parts[1]])
	case len(parts) == 4 && parts[0] == "lists" && parts[2] == "tasks":
		for _, task := range f.tasks[parts[1]] {
			if task.Id == parts[3] {
				writeJSON(w, task)
				return
			}
		}
		writeError(w, http.StatusNotFound)
	default:
		writeError(w, http.StatusNotFound)
	}
}

// delete deletes a list, with its tasks, or a task, with its subtasks, which
// are kept, as Google does, and say they're deleted, with new entity tags
func (f *fakeTasks) delete(w http.ResponseWriter, parts []string) {
	f.deletes = append(f.deletes, "/"+strings.Join(parts, "/"))
	switch {
	case len(parts) == 4 && parts[0] == "users" && parts[2] == "lists":
		for i := range f.lists {
			if f.lists[i].Id == parts[3] {
				f.lists = append(f.lists[:i], f.lists[i+1:]...)
				delete(f.tasks, parts[3])
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
	case len(parts) == 4 && parts[0] == "lists" && parts[2] == "tasks":
		tasks := f.tasks[parts[1]]
		ids := map[string]bool{parts[3]: true}
		found := false
		for changed := true; changed; {
			changed = false
			for i := range tasks {
				if (ids[tasks[i].Id] || ids[tasks[i].Parent]) && !tasks[i].Deleted {
					found = found || tasks[i].Id == parts[3]
					tasks[i].Deleted, tasks[i].ETag = true, tasks[i].Id+"-deleted"
					ids[tasks[i].Id], changed = true, true
				}
			}
		}
		if found {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	writeError(w, http.StatusNotFound)
}

// written is when the fake writes a task, which is its update time, and, of
// a task which is completed without a time, its completion
var written = time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)

// write inserts a task, beneath its parent, with an id, or patches a task's
// fields, where null clears one, or moves a task beneath its parent, as
// Google does, and returns it. A parent which isn't a task of the list is
// a bad request.
func (f *fakeTasks) write(w http.ResponseWriter, r *http.Request, parts []string) {
	f.writes = append(f.writes, r.Method+" /"+strings.Join(parts, "/"))
	if len(parts) < 3 || parts[0] != "lists" || parts[2] != "tasks" || f.list(parts[1]) == nil {
		writeError(w, http.StatusNotFound)
		return
	}
	list := parts[1]
	find := func(id string) *httpclient.Task {
		for i := range f.tasks[list] {
			if task := &f.tasks[list][i]; task.Id == id && !task.Deleted {
				return task
			}
		}
		return nil
	}
	parent := r.URL.Query().Get("parent")
	if parent != "" && find(parent) == nil {
		writeError(w, http.StatusBadRequest)
		return
	}
	var task *httpclient.Task
	switch {
	case r.Method == http.MethodPost && len(parts) == 3:
		f.tasks[list] = append(f.tasks[list], httpclient.Task{Id: "N" + strconv.Itoa(len(f.tasks[list])+1), Parent: parent})
		task = &f.tasks[list][len(f.tasks[list])-1]
	case r.Method == http.MethodPatch && len(parts) == 4:
		if task = find(parts[3]); task == nil {
			writeError(w, http.StatusNotFound)
			return
		}
	case r.Method == http.MethodPost && len(parts) == 5 && parts[4] == "move":
		if f.failMove {
			writeError(w, http.StatusServiceUnavailable)
			return
		} else if task = find(parts[3]); task == nil {
			writeError(w, http.StatusNotFound)
			return
		}
		task.Parent = parent
		task.ETag = task.Id + "-" + strconv.Itoa(len(f.writes))
		writeJSON(w, task)
		return
	default:
		writeError(w, http.StatusNotFound)
		return
	}

	// The fields, where null clears one
	var fields map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
		writeError(w, http.StatusBadRequest)
		return
	}
	f.fields = append(f.fields, fields)
	str := func(key string, value *string) {
		if raw, exists := fields[key]; exists {
			*value = ""
			_ = json.Unmarshal(raw, value)
		}
	}
	at := func(key string, value **time.Time) {
		if raw, exists := fields[key]; exists {
			*value = nil
			_ = json.Unmarshal(raw, value)
		}
	}
	str("title", &task.Title)
	str("notes", &task.Notes)
	str("status", &task.Status)
	at("due", &task.Due)
	at("completed", &task.Completed)
	if task.Status == "completed" && task.Completed == nil {
		task.Completed = &written
	}
	task.Updated, task.ETag = written, task.Id+"-"+strconv.Itoa(len(f.writes))
	writeJSON(w, task)
}

func (f *fakeTasks) list(id string) *httpclient.TaskList {
	for i := range f.lists {
		if f.lists[i].Id == id {
			return &f.lists[i]
		}
	}
	return nil
}

// addList and addTask add a list, and a task of a list
func (f *fakeTasks) addList(id, title string) {
	f.Lock()
	defer f.Unlock()
	f.lists = append(f.lists, httpclient.TaskList{Id: id, ETag: id + "-1", Title: title, Updated: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)})
}

func (f *fakeTasks) addTask(list string, task httpclient.Task) {
	f.Lock()
	defer f.Unlock()
	if task.ETag == "" {
		task.ETag = task.Id + "-1"
	}
	f.tasks[list] = append(f.tasks[list], task)
}

// writePage writes a page of items, of maxResults, from pageToken, which is
// the index of the first
func writePage[T any](w http.ResponseWriter, r *http.Request, items []T) {
	size, _ := strconv.Atoi(r.URL.Query().Get("maxResults"))
	start, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
	if size <= 0 || size > 100 {
		size = 100
	}
	end := min(start+size, len(items))
	page := map[string]any{"items": items[start:end]}
	if end < len(items) {
		page["nextPageToken"] = strconv.Itoa(end)
	}
	writeJSON(w, page)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": status, "message": http.StatusText(status)}})
}

// newFiler returns a filer of the fake, with the token, and the parameters
func Test_TaskDescription(t *testing.T) {
	fake, endpoint := newFake(t)

	// A volume's description is of the user who signed in, when it can be
	// read, or else the kind of volume, and is its parameter otherwise
	assert.Equal(t, "Google Tasks", newFiler(t, endpoint, nil).URL().Query().Get("description"))
	fake.Lock()
	fake.email = "ada@example.com"
	fake.Unlock()
	filer := newFiler(t, endpoint, nil)
	assert.Equal(t, "Google Tasks for ada@example.com", filer.URL().Query().Get("description"))
	assert.Equal(t, "Shopping", newFiler(t, endpoint, url.Values{"description": {"Shopping"}}).URL().Query().Get("description"))

	// A clone keeps it, and one without it is described again
	clone, err := filer.Clone(context.Background(), url.Values{"hidden": {"true"}})
	require.NoError(t, err)
	assert.Equal(t, "Google Tasks for ada@example.com", clone.URL().Query().Get("description"))

	// As does a clone of the same sign-in, of its credential, or the default
	shopping := newFiler(t, endpoint, url.Values{"description": {"Shopping"}})
	for _, credential := range [][]string{{google.DefaultCredential}, {""}, {}} {
		clone, err := shopping.Clone(context.Background(), url.Values{"credential": credential})
		require.NoError(t, err)
		assert.Equal(t, "Shopping", clone.URL().Query().Get("description"), credential)
	}
	fake.Lock()
	fake.email = "grace@example.com"
	fake.Unlock()
	clone, err = filer.Clone(context.Background(), url.Values{"description": {}})
	require.NoError(t, err)
	assert.Equal(t, "Google Tasks for grace@example.com", clone.URL().Query().Get("description"))
}

func newFiler(t *testing.T, endpoint string, params url.Values) *google.TaskFiler {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	if !params.Has("quota") {
		// Unlimited, unless a test sets it, so the tests aren't paced
		params.Set("quota", "0")
	}
	filer, err := newTaskFiler(t.Context(), "tasks", params, newCredentials(t, endpoint, nil))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, filer.Close()) })
	return filer
}

// list returns the paths of a listing
func list(t *testing.T, filer gofiler.Filer, req schema.ListObjectsRequest) []string {
	t.Helper()
	iter, err := filer.ListObjects(t.Context(), req)
	require.NoError(t, err)
	defer iter.Close()
	var paths []string
	for {
		object, err := iter.Next(t.Context())
		if errors.Is(err, io.EOF) {
			return paths
		}
		require.NoError(t, err)
		paths = append(paths, object.Path)
	}
}

////////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_NewTaskFiler(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFake(t)

	var err error
	// Its parameters, each of which is its URL's, with those which aren't
	// set at their defaults
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}, "quota": {"0"}, "refresh_interval": {"30s"}})
	assert.False(t, filer.Readonly())
	u := filer.URL()
	assert.Equal(t, google.TasksScheme, u.Scheme)
	assert.Equal(t, "tasks", u.Host)
	assert.Equal(t, "false", u.Query().Get("readonly"))
	assert.Equal(t, "0", u.Query().Get("quota"))
	assert.Equal(t, "30s", u.Query().Get("refresh_interval"))
	assert.Equal(t, google.DefaultCredential, u.Query().Get("credential"))
	assert.Equal(t, "false", u.Query().Get("hidden"))
	assert.True(t, newFiler(t, endpoint, nil).Readonly(), "read-only by default")

	// Its quota spaces its requests evenly: at 600 a minute, one each 100ms,
	// so two requests after the check take at least 150ms, with some slack
	paced := newFiler(t, endpoint, url.Values{"quota": {"600"}})
	start := time.Now()
	for range 2 {
		_, err := paced.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L9"}})
		assert.ErrorIs(t, err, fs.ErrNotExist)
	}
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)

	// A clone, with a patch, and the same token
	clone, err := filer.Clone(ctx, url.Values{"readonly": nil})
	require.NoError(t, err)
	assert.Equal(t, "true", clone.URL().Query().Get("readonly"), "it's the default")

	// Parameters which can't be parsed, or aren't known, and a name which
	// isn't an identifier
	for _, params := range []url.Values{
		{"readonly": {"maybe"}}, {"quota": {"-1"}}, {"refresh_interval": {"soon"}},
		{"refresh_interval": {"-1s"}}, {"hidden": {"maybe"}}, {"credential": {"GOOGLE_CLIENT"}}, {"credential": {"S3_SECRET"}}, {"credential": {"not a key"}}, {"endpoint": {"ftp://example.com"}}, {"colour": {"blue"}},
	} {
		_, err := newTaskFiler(ctx, "tasks", params, nil)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}
	_, err = newTaskFiler(ctx, "tasks", url.Values{"credential": {"not a key"}}, nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = newTaskFiler(ctx, "not a name", nil, nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_TaskObjects(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	due := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	completed := time.Date(2026, 11, 18, 9, 30, 0, 0, time.UTC)
	fake.addList("L1", "Shopping")
	fake.addTask("L1", httpclient.Task{Id: "T1", Title: "Buy  milk\n", Notes: "Semi-skimmed, <two> pints.", Status: "needsAction", Due: &due, Updated: completed, WebViewLink: "https://tasks.google.com/task/T1"})
	fake.addTask("L1", httpclient.Task{Id: "T2", Title: "Bread", Status: "completed", Completed: &completed, Hidden: true, Parent: "T1", Links: []httpclient.TaskLink{{Type: "email", Description: "From Sam", Link: "https://mail.google.com/x"}}})
	fake.addTask("L1", httpclient.Task{Id: "T3", Title: "Gone", Deleted: true})
	filer := newFiler(t, endpoint, url.Values{"hidden": {"true"}})

	// The root, and a list, are directories, with its title
	root, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	assert.True(t, root.IsDir)
	dir, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/"}})
	require.NoError(t, err)
	assert.True(t, dir.IsDir)
	assert.Equal(t, "/L1", dir.Path)
	assert.Equal(t, []schema.Meta{
		{Namespace: google.TaskNamespace, Key: "title", Value: json.RawMessage(`"Shopping"`)},
		{Namespace: google.DCNamespace, Key: "title", Value: json.RawMessage(`"Shopping"`)},
		{Namespace: schema.FolderNamespace, Key: "children", Value: json.RawMessage(`true`)},
		{Namespace: schema.FolderNamespace, Key: "children-count", Value: json.RawMessage(`2`)},
	}, dir.Meta, "with its tasks, which aren't deleted")

	// A task is a file, of iCalendar, with its metadata, its entity tag and
	// when it was updated
	file, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T1.ics"}})
	require.NoError(t, err)
	assert.False(t, file.IsDir)
	assert.Equal(t, ical.TaskType, file.ContentType)
	assert.Equal(t, "T1-1", *file.ETag)
	assert.Equal(t, completed, file.ModTime)
	meta := make(map[string]string)
	for _, m := range file.Meta {
		var value string
		require.NoError(t, json.Unmarshal(m.Value, &value))
		meta[m.Namespace+":"+m.Key] = value
	}
	assert.Equal(t, map[string]string{"task:title": "Buy  milk\n", "dc:title": "Buy  milk\n", "task:status": "needsAction", "task:due": "2026-11-20", "dc:identifier": "T1", "task:url": "https://tasks.google.com/task/T1"}, meta)

	// Its content, whose size is its file's
	read, object, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T1.ics"}})
	require.NoError(t, err)
	var content string
	require.NoError(t, read(ctx, func(r io.Reader) error {
		data, err := io.ReadAll(r)
		content = string(data)
		return err
	}))
	// It's an iCalendar, of a VTODO, with its id, title, on one line, notes,
	// due date, status, link, and when it was updated
	assert.True(t, strings.HasPrefix(content, "BEGIN:VCALENDAR\r\n"))
	for _, line := range []string{"PRODID:" + google.ProdID, "BEGIN:VTODO", "UID:T1", "DUE;VALUE=DATE:20261120", "STATUS:NEEDS-ACTION", "LAST-MODIFIED:20261118T093000Z"} {
		assert.Contains(t, content, line+"\r\n")
	}
	cal, err := ical.Decode(strings.NewReader(content))
	require.NoError(t, err)
	require.Len(t, cal.Events, 1)
	todo := cal.Events[0]
	assert.Equal(t, ical.KindTask, todo.Kind)
	assert.Equal(t, "T1", todo.UID)
	assert.Equal(t, "Buy milk", todo.Summary)
	assert.Equal(t, "Semi-skimmed, <two> pints.", todo.Description)
	assert.Equal(t, ical.Date(2026, 11, 20), todo.Due)
	assert.Equal(t, "NEEDS-ACTION", todo.Status)
	assert.Equal(t, "https://tasks.google.com/task/T1", todo.URL)
	assert.True(t, todo.Modified.Equal(completed))
	assert.True(t, todo.Completed.IsZero())
	assert.Empty(t, todo.Parent)
	assert.Equal(t, int64(len(content)), *object.Size)

	// Of a completed subtask, with links
	read, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T2.ics"}})
	require.NoError(t, err)
	require.NoError(t, read(ctx, func(r io.Reader) error {
		data, err := io.ReadAll(r)
		content = string(data)
		return err
	}))
	cal, err = ical.Decode(strings.NewReader(content))
	require.NoError(t, err)
	require.Len(t, cal.Events, 1)
	todo = cal.Events[0]
	assert.Equal(t, "Bread", todo.Summary)
	assert.Equal(t, "COMPLETED", todo.Status)
	assert.True(t, todo.Completed.Equal(completed))
	assert.Equal(t, "T1", todo.Parent)
	assert.True(t, todo.Due.IsZero() && todo.Start.IsZero(), "it isn't due")

	// What an iCalendar can't have is its metadata: it's hidden, and its
	// links
	file, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T2.ics"}})
	require.NoError(t, err)
	extra := make(map[string]string)
	for _, m := range file.Meta {
		if m.Key == "hidden" || m.Key == "links" {
			extra[m.Key] = string(m.Value)
		}
	}
	assert.Equal(t, map[string]string{"hidden": "true", "links": `[{"type":"email","description":"From Sam","link":"https://mail.google.com/x"}]`}, extra)

	// Not modified, of its entity tag
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T1.ics"}, IfNoneMatch: []string{"T1-1"}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)

	// What doesn't exist: a list, a task, a deleted task, a task as a
	// directory, a file which isn't Markdown, and a deeper path
	for _, path := range []string{"/L9", "/L1/T9.ics", "/L1/T3.ics", "/L1/T1.ics/", "/L1/T1.txt", "/L1/T1/x.ics", "/L1/.ics"} {
		_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}

	// A directory can't be read
	_, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)

	// A read-only volume can't be written, and one which isn't can't yet
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T4.ics"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T1.ics"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	writable := newFiler(t, endpoint, url.Values{"readonly": {"false"}})
	_, err = writable.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T4.ics"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter, "without an iCalendar")
}

func Test_TaskList(t *testing.T) {
	fake, endpoint := newFake(t)
	fake.addList("L2", "Work")
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "B"})
	fake.addTask("L1", httpclient.Task{Id: "A"})
	fake.addTask("L1", httpclient.Task{Id: "C", Deleted: true})
	fake.addTask("L2", httpclient.Task{Id: "X"})
	filer := newFiler(t, endpoint, nil)

	// The lists, and, recursively, each followed by its tasks, in the order
	// of their paths
	assert.Equal(t, []string{"/L1", "/L2"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}))
	assert.Equal(t, []string{"/L1", "/L1/A.ics", "/L1/B.ics", "/L2", "/L2/X.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))

	// Each list's directory has the count of its tasks, which aren't
	// deleted, and whether it has any
	fake.addList("L3", "Empty")
	counts := map[string]string{}
	iter, err := filer.ListObjects(t.Context(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	for {
		object, err := iter.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		for _, m := range object.Meta {
			if m.Namespace == schema.FolderNamespace {
				counts[object.Path+" "+m.Key] = string(m.Value)
			}
		}
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, map[string]string{
		"/L1 children": "true", "/L1 children-count": "2",
		"/L2 children": "true", "/L2 children-count": "1",
		"/L3 children": "false", "/L3 children-count": "0",
	}, counts)
	fake.Lock()
	fake.lists = fake.lists[:2]
	fake.Unlock()

	// A list's tasks, and only a task
	assert.Equal(t, []string{"/L1/A.ics", "/L1/B.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/"}}))
	assert.Equal(t, []string{"/L2/X.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L2/X.ics"}}))

	// After a path: within a list, after a list's directory, and after a
	// list's tasks, which aren't read
	assert.Equal(t, []string{"/L1/B.ics", "/L2", "/L2/X.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: "/L1/A.ics"}))
	assert.Equal(t, []string{"/L1/A.ics", "/L1/B.ics", "/L2", "/L2/X.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: "/L1/"}))
	fake.Lock()
	before := fake.requests
	fake.Unlock()
	assert.Equal(t, []string{"/L2/X.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: "/L2/"}))
	fake.Lock()
	assert.Equal(t, before+2, fake.requests, "the lists, and L2's tasks, but not L1's")
	fake.Unlock()

	// A list which doesn't exist
	_, err = filer.ListObjects(t.Context(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L9/"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func Test_TaskListPages(t *testing.T) {
	fake, endpoint := newFake(t)

	// More lists, and tasks, than a page, which are all listed
	var want []string
	for i := range 3 {
		fake.addList(fmt.Sprintf("L%d", i), "")
	}
	for i := range 250 {
		id := fmt.Sprintf("T%03d", i)
		fake.addTask("L1", httpclient.Task{Id: id})
		want = append(want, "/L1/"+id+".ics")
	}
	filer := newFiler(t, endpoint, nil)
	assert.Equal(t, want, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1"}}))
	assert.Len(t, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}), 253)
}

func Test_TaskWatch(t *testing.T) {
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "A"})
	fake.addTask("L1", httpclient.Task{Id: "B"})
	filer := newFiler(t, endpoint, url.Values{"refresh_interval": {"20ms"}})

	// Watching, once what's there is read
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The first change is a rescan of the root, once what's there is read
	events, done := make(chan schema.Event, 16), make(chan error, 1)
	go func() { done <- filer.WatchObjects(ctx, events) }()
	select {
	case event := <-events:
		assert.Equal(t, schema.Rescan("tasks"), event)
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}

	// A task which is created, one which is updated, one which is deleted,
	// and a list which is created, with a task
	fake.Lock()
	fake.tasks["L1"][0].ETag = "A-2"
	fake.tasks["L1"] = slices.Delete(fake.tasks["L1"], 1, 2)
	fake.tasks["L1"] = append(fake.tasks["L1"], httpclient.Task{Id: "C", ETag: "C-1"})
	fake.lists = append(fake.lists, httpclient.TaskList{Id: "L2", ETag: "L2-1"})
	fake.tasks["L2"] = []httpclient.Task{{Id: "X", ETag: "X-1"}}
	fake.Unlock()
	var got []string
	for len(got) < 5 {
		select {
		case event := <-events:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for events, got %v", got)
		}
	}
	assert.Equal(t, []string{"deleted /L1/B.ics", "updated /L1/A.ics", "created /L1/C.ics", "created /L2", "created /L2/X.ics"}, got)

	// A list which is deleted is the deletion of its tasks, then it
	fake.Lock()
	fake.lists = fake.lists[:1]
	fake.Unlock()
	got = nil
	for len(got) < 2 {
		select {
		case event := <-events:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for events, got %v", got)
		}
	}
	assert.Equal(t, []string{"deleted /L2/X.ics", "deleted /L2"}, got)

	// A task which is added to a list updates its directory too, as the
	// count of its tasks changes
	fake.Lock()
	fake.tasks["L1"] = append(fake.tasks["L1"], httpclient.Task{Id: "D", ETag: "D-1"})
	fake.Unlock()
	got = nil
	for len(got) < 2 {
		select {
		case event := <-events:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for events, got %v", got)
		}
	}
	assert.Equal(t, []string{"updated /L1", "created /L1/D.ics"}, got)

	// Until it's cancelled
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)

	// Without a refresh interval, it isn't watched
	unwatched := newFiler(t, endpoint, url.Values{"refresh_interval": {"0"}})
	assert.ErrorIs(t, unwatched.WatchObjects(t.Context(), events), gofiler.ErrNotImplemented)
}

func Test_TaskListOrder(t *testing.T) {
	fake, endpoint := newFake(t)

	// Ids whose order changes once a "/", or ".ics", follows them, which are
	// listed in the order of their paths, so a page, of one object, which
	// starts after the last, misses none
	fake.addList("A", "")
	fake.addList("A-", "")
	fake.addTask("A", httpclient.Task{Id: "T"})
	fake.addTask("A", httpclient.Task{Id: "T-"})
	filer := newFiler(t, endpoint, nil)
	want := []string{"/A-", "/A", "/A/T-.ics", "/A/T.ics"}
	assert.Equal(t, want, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))
	var paged []string
	after := ""
	for {
		iter, err := filer.ListObjects(t.Context(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: after})
		require.NoError(t, err)
		object, err := iter.Next(t.Context())
		require.NoError(t, iter.Close())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		paged = append(paged, object.Path)
		after = object.Path
		if object.IsDir {
			after += "/"
		}
	}
	assert.Equal(t, want, paged)

	// Tasks which are assigned, such as from Docs, are asked for
	fake.Lock()
	defer fake.Unlock()
	require.NotEmpty(t, fake.queries)
	for _, query := range fake.queries {
		assert.Equal(t, "true", query.Get("showAssigned"))
	}
}

func Test_TaskReadCancelled(t *testing.T) {
	fake, endpoint := newFake(t)
	fake.addList("L1", "")
	fake.addTask("L1", httpclient.Task{Id: "T1", Title: "A task", Notes: strings.Repeat("notes ", 100)})
	filer := newFiler(t, endpoint, nil)
	read, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/T1.ics"}})
	require.NoError(t, err)

	// It can seek, so it's read in ranges, until the context is cancelled,
	// when the rest of it isn't read
	ctx, cancel := context.WithCancel(t.Context())
	err = read(ctx, func(r io.Reader) error {
		seeker, ok := r.(io.Seeker)
		require.True(t, ok)
		_, err := seeker.Seek(4, io.SeekStart)
		require.NoError(t, err)
		buf := make([]byte, 10)
		_, err = r.Read(buf)
		require.NoError(t, err)
		_, isWriterTo := r.(io.WriterTo)
		assert.False(t, isWriterTo, "it isn't copied without the context being checked")
		cancel()
		_, err = io.ReadAll(r)
		return err
	})
	assert.ErrorIs(t, err, context.Canceled)
}

// status returns the HTTP status of an error, as the root's HTTPErr maps
// it, of go-filer's errors, or go-auth's
func status(err error) int {
	var code httpresponse.Err
	if errors.As(gofiler.HTTPErr(err), &code) {
		return int(code)
	}
	return 0
}

func Test_TaskSignIn(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	params := func(values ...string) url.Values {
		result := url.Values{"quota": {"0"}}
		for i := 0; i+1 < len(values); i += 2 {
			result.Set(values[i], values[i+1])
		}
		return result
	}
	signIn := func(t *testing.T, err error) *authclient.SignIn {
		t.Helper()
		require.Equal(t, http.StatusUnauthorized, status(err))
		signIn := authclient.SignInOf(err)
		require.NotNil(t, signIn)
		assert.Equal(t, google.Google, signIn.Server)
		assert.Equal(t, clientID, signIn.ClientID)
		return signIn
	}

	// Without a sign-in, or a client, or a provider of credentials
	creds := newCredentials(t, endpoint, nil)
	delete(creds.values, google.DefaultCredential)
	_, err := newTaskFiler(ctx, "tasks", params(), creds)
	need := signIn(t, err)
	assert.Equal(t, google.DefaultCredential, need.Credential)
	assert.Equal(t, []string{httpclient.TasksReadonlyScope, httpclient.EmailScope}, need.Scopes, "a read-only volume asks for read-only access, and the user's email address")
	creds = newCredentials(t, endpoint, nil)
	delete(creds.values, google.ClientCredential)
	_, err = newTaskFiler(ctx, "tasks", params(), creds)
	assert.Equal(t, http.StatusServiceUnavailable, status(err), "no one can sign in, without a client")
	_, err = newTaskFiler(ctx, "tasks", params(), nil)
	assert.Equal(t, http.StatusServiceUnavailable, status(err))

	// Of another credential, which is the parameter's
	creds = newCredentials(t, endpoint, nil)
	_, err = newTaskFiler(ctx, "tasks", params("credential", "GOOGLE_WORK"), creds)
	assert.Equal(t, "GOOGLE_WORK", signIn(t, err).Credential)

	// For another client, which is signed in to again
	_, err = newTaskFiler(ctx, "tasks", params(), newCredentials(t, endpoint, func(c *authclient.Grant) { c.Client.ClientID = "client-2" }))
	signIn(t, err)

	// Of read-only access, for a volume which isn't read-only, which asks for
	// the access it needs, and keeps what the sign-in has
	_, err = newTaskFiler(ctx, "tasks", params("readonly", "false"), newCredentials(t, endpoint, func(c *authclient.Grant) {
		c.Scopes = []string{httpclient.TasksReadonlyScope, "https://www.googleapis.com/auth/calendar"}
	}))
	assert.Equal(t, []string{httpclient.TasksScope, httpclient.EmailScope, httpclient.TasksReadonlyScope, "https://www.googleapis.com/auth/calendar"}, signIn(t, err).Scopes)

	// Whose token Google doesn't accept, such as when it's revoked
	_, err = newTaskFiler(ctx, "tasks", params(), newCredentials(t, endpoint, func(c *authclient.Grant) { c.Token.AccessToken = "revoked" }))
	signIn(t, err)

	// Whose token has expired, which is refreshed, and stored
	creds = newCredentials(t, endpoint, func(c *authclient.Grant) { c.Token.Expiry = time.Now().Add(-time.Minute); c.Token.AccessToken = "old" })
	filer, err := newTaskFiler(ctx, "tasks", params(), creds)
	require.NoError(t, err)
	assert.Equal(t, []string{"/L1"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}))
	stored := creds.credential(t, google.DefaultCredential)
	assert.Equal(t, token, stored.Token.AccessToken)
	assert.True(t, stored.Token.Expiry.After(time.Now().Add(30*time.Minute)))
	assert.Equal(t, "refresh", stored.Token.RefreshToken, "its refresh token is kept")
	fake.Lock()
	refreshes := fake.refreshes
	fake.Unlock()
	list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	fake.Lock()
	assert.Equal(t, refreshes, fake.refreshes, "it isn't refreshed again, until it expires")
	fake.Unlock()

	// Whose client Google refuses, such as with the wrong secret, which
	// signing in again doesn't mend
	wrong := newCredentials(t, endpoint, func(c *authclient.Grant) { c.Token.Expiry = time.Now().Add(-time.Minute) })
	wrong.values[google.ClientCredential] = json.RawMessage(`{"client_id":"` + clientID + `","client_secret":"wrong-secret"}`)
	_, err = newTaskFiler(ctx, "tasks", params(), wrong)
	assert.Equal(t, http.StatusServiceUnavailable, status(err))
	assert.Nil(t, authclient.SignInOf(err), "it isn't signed in to again")

	// Whose refresh token is revoked
	_, err = newTaskFiler(ctx, "tasks", params(), newCredentials(t, endpoint, func(c *authclient.Grant) {
		c.Token.Expiry, c.Token.RefreshToken = time.Now().Add(-time.Minute), "revoked"
	}))
	signIn(t, err)

	// Whose client isn't one
	bad := newCredentials(t, endpoint, nil)
	bad.values[google.ClientCredential] = json.RawMessage(`{"installed":{}}`)
	_, err = newTaskFiler(ctx, "tasks", params(), bad)
	assert.Equal(t, http.StatusBadRequest, status(err))
}

func Test_TaskSignInOptions(t *testing.T) {
	// The URL to sign in at asks for a refresh token
	u, err := url.Parse(authclient.SignIn{Server: google.Google, ClientID: clientID, Scopes: []string{httpclient.TasksScope}}.AuthCodeURL("http://127.0.0.1:8085/callback", "state", "verifier", google.SignInOptions...))
	require.NoError(t, err)
	assert.Equal(t, "offline", u.Query().Get("access_type"))
	assert.Equal(t, "consent", u.Query().Get("prompt"))
	assert.Equal(t, "true", u.Query().Get("include_granted_scopes"))
}

func Test_TaskListValidators(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "A", Updated: time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)})
	filer := newFiler(t, endpoint, nil)
	get := func(req schema.GetObjectRequest) (*schema.Object, error) {
		req.Path = "/L1"
		return filer.GetObject(ctx, req)
	}

	// A list's directory is modified when its tasks are, after its own
	// update, so its entity tag, and its modification time, change, though
	// the list's don't
	dir, err := get(schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC), dir.ModTime, "of its task, after the list's update")
	_, err = get(schema.GetObjectRequest{IfNoneMatch: []string{*dir.ETag}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)
	fake.addTask("L1", httpclient.Task{Id: "B", Updated: time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)})
	_, err = get(schema.GetObjectRequest{IfNoneMatch: []string{*dir.ETag}})
	assert.NoError(t, err, "of a task which is added")
	_, err = get(schema.GetObjectRequest{IfModifiedSince: dir.ModTime})
	assert.NoError(t, err)

	// A task which is deleted modifies it too, when it was deleted
	dir, err = get(schema.GetObjectRequest{})
	require.NoError(t, err)
	fake.Lock()
	fake.tasks["L1"][0].Deleted, fake.tasks["L1"][0].Updated = true, time.Date(2026, 11, 18, 15, 0, 0, 0, time.UTC)
	fake.Unlock()
	_, err = get(schema.GetObjectRequest{IfNoneMatch: []string{*dir.ETag}})
	assert.NoError(t, err)
	_, err = get(schema.GetObjectRequest{IfModifiedSince: dir.ModTime})
	assert.NoError(t, err)

	// A listing of a list, after its tasks, doesn't read them
	fake.Lock()
	before := fake.requests
	fake.Unlock()
	assert.Empty(t, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/"}, StartAfter: "/L2/"}))
	fake.Lock()
	assert.Equal(t, before+1, fake.requests, "only the list, which is checked")
	fake.Unlock()
}

func Test_TaskDelete(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addList("L2", "Work")
	fake.addTask("L1", httpclient.Task{Id: "A", Title: "Parent"})
	fake.addTask("L1", httpclient.Task{Id: "B", Title: "Child", Parent: "A"})
	fake.addTask("L1", httpclient.Task{Id: "C", Title: "Other"})
	fake.addTask("L1", httpclient.Task{Id: "D", Title: "Grandchild", Parent: "B"})
	fake.addTask("L1", httpclient.Task{Id: "E", Title: "Deleted child", Parent: "A", Deleted: true})
	fake.addTask("L2", httpclient.Task{Id: "X"})
	fake.addTask("L2", httpclient.Task{Id: "Y", Deleted: true})
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}})
	paths := func(resp *schema.DeleteObjectsResponse) []string {
		var result []string
		for _, object := range resp.Body {
			result = append(result, object.Path)
		}
		return result
	}

	// A task is deleted, with its subtasks, as Google does, which aren't
	// listed, nor found
	resp, err := filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"/L1/A.ics", "/L1/B.ics", "/L1/D.ics"}, paths(resp), "with its subtasks, and theirs, but not those already deleted")
	assert.Equal(t, []string{"/L1/C.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/"}}))
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/B.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	fake.Lock()
	assert.Equal(t, []string{"/lists/L1/tasks/A"}, fake.deletes, "Google deletes the subtasks")
	fake.Unlock()

	// A task which is deleted, or doesn't exist, or a path of a task which is
	// a directory's, isn't found
	for _, path := range []string{"/L1/A.ics", "/L1/Z.ics", "/L2/Y.ics", "/L1/C.ics/", "/L9/X.ics"} {
		_, err := filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}

	// A list is deleted, with its tasks, which aren't deleted, only when the
	// request is recursive, and the root isn't
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L2"}})
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	resp, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L2/"}, Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/L2/X.ics", "/L2"}, paths(resp), "its tasks, and then the list")
	assert.True(t, resp.Body[1].IsDir)
	assert.Equal(t, []string{"/L1", "/L1/C.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L2"}, Recursive: true})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	for _, path := range []string{"/L2", "/L9"} {
		_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, "a list which doesn't exist isn't found, rather than a conflict: %s", path)
	}
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// An empty list is deleted too, which is returned
	fake.addList("L3", "Empty")
	resp, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L3"}, Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/L3"}, paths(resp))
}

func Test_TaskWatchDeleted(t *testing.T) {
	// watchEvents watches the filer, once it's read what's there, and
	// returns its events, after its rescan
	watchEvents := func(t *testing.T, filer *google.TaskFiler) <-chan schema.Event {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		events, done := make(chan schema.Event, 16), make(chan error, 1)
		go func() { done <- filer.WatchObjects(ctx, events) }()
		t.Cleanup(func() { cancel(); <-done })
		select {
		case event := <-events:
			require.Equal(t, schema.Rescan("tasks"), event)
		case <-time.After(5 * time.Second):
			t.Fatal("no rescan")
		}
		return events
	}
	next := func(t *testing.T, events <-chan schema.Event) string {
		t.Helper()
		select {
		case event := <-events:
			return event.EventType.String() + " " + event.Path
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
			return ""
		}
	}

	// What the filer deletes is sent as it's deleted, without waiting to
	// refresh: a task, with its subtask, and a list, with its tasks, and
	// then the list
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addList("L2", "Work")
	fake.addTask("L1", httpclient.Task{Id: "A"})
	fake.addTask("L1", httpclient.Task{Id: "B", Parent: "A"})
	fake.addTask("L2", httpclient.Task{Id: "X"})
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}, "refresh_interval": {"1h"}})
	events := watchEvents(t, filer)
	_, err := filer.DeleteObjects(t.Context(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}})
	require.NoError(t, err)
	assert.Equal(t, "deleted /L1/A.ics", next(t, events))
	assert.Equal(t, "deleted /L1/B.ics", next(t, events))
	_, err = filer.DeleteObjects(t.Context(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L2"}, Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, "deleted /L2/X.ics", next(t, events))
	assert.Equal(t, "deleted /L2", next(t, events))

	// And isn't sent again when it's refreshed, though the list whose task
	// is deleted is updated, as its count of tasks is
	fake, endpoint = newFake(t)
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "A"})
	fake.addTask("L1", httpclient.Task{Id: "C"})
	filer = newFiler(t, endpoint, url.Values{"readonly": {"false"}, "refresh_interval": {"50ms"}})
	events = watchEvents(t, filer)
	_, err = filer.DeleteObjects(t.Context(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}})
	require.NoError(t, err)
	assert.Equal(t, "deleted /L1/A.ics", next(t, events))
	assert.Equal(t, "updated /L1", next(t, events))
	select {
	case event := <-events:
		t.Fatalf("unexpected %s %s", event.EventType.String(), event.Path)
	case <-time.After(300 * time.Millisecond):
	}
}

// vtodo returns an iCalendar of a VTODO, of its properties
func vtodo(lines ...string) io.Reader {
	return strings.NewReader("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTODO\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VTODO\r\nEND:VCALENDAR\r\n")
}

func Test_TaskWrite(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	due := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	fake.addTask("L1", httpclient.Task{Id: "A", Title: "Shopping", Notes: "Milk", Status: "needsAction", Due: &due, Links: []httpclient.TaskLink{{Type: "email", Link: "https://mail.google.com/x"}}})
	fake.addTask("L1", httpclient.Task{Id: "C", Title: "Errands", Status: "needsAction"})
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}})
	write := func(path string, body io.Reader) (*schema.Object, error) {
		return filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: path}, Body: body, ObjectMeta: schema.ObjectMeta{ContentType: ical.TaskType}})
	}
	task := func(id string) httpclient.Task {
		fake.Lock()
		defer fake.Unlock()
		for _, task := range fake.tasks["L1"] {
			if task.Id == id {
				return task
			}
		}
		t.Fatalf("no task %q", id)
		return httpclient.Task{}
	}

	// The PUT handler gets ~new.ics first, to know whether it's created,
	// which isn't found, as it can't be an id of Google's, rather than
	// asked for, which Google refuses as a bad request
	_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/~new.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	for _, path := range []string{"/L1/~other.ics", "/L~1/A.ics", "/L~1"} {
		_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
		_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}, Recursive: true})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
		_, err = write(path, vtodo("SUMMARY:x"))
		assert.Error(t, err, path)
	}
	_, err = write("/L1/~other.ics", vtodo("SUMMARY:x"))
	assert.ErrorIs(t, err, fs.ErrNotExist, "only ~new creates a task")

	// Writing ~new.ics creates a task, which is at the path of its id, with
	// its fields, and a due date, which is its day
	object, err := write("/L1/~new.ics", vtodo("SUMMARY:Buy eggs", "DESCRIPTION:A dozen", "DUE;TZID=Europe/Berlin:20261121T230000"))
	require.NoError(t, err)
	assert.Equal(t, "/L1/N3.ics", object.Path)
	assert.Equal(t, ical.TaskType, object.ContentType)
	created := task("N3")
	assert.Equal(t, "Buy eggs", created.Title)
	assert.Equal(t, "A dozen", created.Notes)
	assert.Equal(t, "needsAction", created.Status)
	require.NotNil(t, created.Due)
	assert.Equal(t, time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC), created.Due.UTC(), "the day it's due, where it's due")
	assert.Contains(t, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/"}}), "/L1/N3.ics")

	// Beneath its parent
	object, err = write("/L1/~new.ics", vtodo("SUMMARY:Bread", "RELATED-TO:A"))
	require.NoError(t, err)
	assert.Equal(t, "A", task(strings.TrimSuffix(path.Base(object.Path), ".ics")).Parent)

	// Writing a task's path replaces the fields a VTODO has, clearing those
	// it doesn't have, such as its notes and due date, and keeps the
	// others, such as its links, and it's completed, when it's completed
	object, err = write("/L1/A.ics", vtodo("UID:A", "SUMMARY:Shopping, done", "STATUS:COMPLETED"))
	require.NoError(t, err)
	assert.Equal(t, "/L1/A.ics", object.Path)
	updated := task("A")
	assert.Equal(t, "Shopping, done", updated.Title)
	assert.Empty(t, updated.Notes)
	assert.Nil(t, updated.Due)
	assert.Equal(t, "completed", updated.Status)
	require.NotNil(t, updated.Completed)
	assert.Len(t, updated.Links, 1, "its links are kept")
	fake.Lock()
	assert.Equal(t, json.RawMessage("null"), fake.fields[len(fake.fields)-1]["due"], "a due date it doesn't have is cleared")
	fake.Unlock()

	// It's opened again, which clears when it was completed, and moved
	// beneath another task, and back to the top
	_, err = write("/L1/A.ics", vtodo("SUMMARY:Shopping", "STATUS:NEEDS-ACTION", "RELATED-TO:C"))
	require.NoError(t, err)
	updated = task("A")
	assert.Equal(t, "needsAction", updated.Status)
	assert.Nil(t, updated.Completed)
	assert.Equal(t, "C", updated.Parent)
	_, err = write("/L1/A.ics", vtodo("SUMMARY:Shopping"))
	require.NoError(t, err)
	assert.Empty(t, task("A").Parent)
	fake.Lock()
	assert.Contains(t, fake.writes, "POST /lists/L1/tasks/A/move", "a parent which changes moves it")
	fake.Unlock()

	// Errors
	for name, test := range map[string]struct {
		path string
		body io.Reader
		want error
	}{
		"NotATask":     {"/L1/Z.ics", vtodo("SUMMARY:x"), fs.ErrNotExist},
		"AList":        {"/L1", vtodo("SUMMARY:x"), gofiler.ErrBadParameter},
		"OtherUID":     {"/L1/A.ics", vtodo("UID:C", "SUMMARY:x"), gofiler.ErrBadParameter},
		"Cancelled":    {"/L1/A.ics", vtodo("SUMMARY:x", "STATUS:CANCELLED"), gofiler.ErrBadParameter},
		"NoParent":     {"/L1/~new.ics", vtodo("SUMMARY:x", "RELATED-TO:Z"), gofiler.ErrBadParameter},
		"Event":        {"/L1/~new.ics", strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20261121T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"), gofiler.ErrBadParameter},
		"TwoTasks":     {"/L1/~new.ics", strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nEND:VTODO\r\nBEGIN:VTODO\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"), gofiler.ErrBadParameter},
		"NotICalendar": {"/L1/~new.ics", strings.NewReader("not a calendar"), gofiler.ErrBadParameter},
		"NoList":       {"/L9/~new.ics", vtodo("SUMMARY:x"), fs.ErrNotExist},
	} {
		_, err := write(test.path, test.body)
		assert.ErrorIs(t, err, test.want, name)
	}
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/~new.ics"}, Body: vtodo("SUMMARY:x"), ObjectMeta: schema.ObjectMeta{ContentType: "text/plain"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter, "another content type")
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}, Body: vtodo("SUMMARY:x"), IfNotExists: true})
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/~new.ics"}, Body: vtodo("SUMMARY:x"), IfExists: true})
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/Z.ics"}, Body: vtodo("SUMMARY:x"), IfExists: true})
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed, "a task which doesn't exist, which must")

	// A binary upload is a task, as its content is
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/C.ics"}, Body: vtodo("SUMMARY:Errands"), ObjectMeta: schema.ObjectMeta{ContentType: "application/octet-stream"}})
	assert.NoError(t, err)

	// What isn't valid doesn't change the task: a parent which isn't a task
	// of the list, a date which isn't one, which would clear its due date,
	// or an event, which isn't decoded, as it has no start
	before := task("C")
	fake.Lock()
	writes := len(fake.writes)
	fake.Unlock()
	for name, body := range map[string]io.Reader{
		"Parent":    vtodo("SUMMARY:Changed", "RELATED-TO:Z"),
		"Itself":    vtodo("SUMMARY:Changed", "RELATED-TO:C"),
		"Due":       vtodo("SUMMARY:Changed", "DUE;VALUE=DATE:20260230"),
		"Completed": vtodo("SUMMARY:Changed", "STATUS:COMPLETED", "COMPLETED:yesterday"),
		"Event":     strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:No start\r\nEND:VEVENT\r\nBEGIN:VTODO\r\nSUMMARY:Changed\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"),
		"Unclosed":  strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nSUMMARY:Changed\r\nEND:VCALENDAR\r\n"),
		"Nested":    strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nSUMMARY:Changed\r\nBEGIN:VTODO\r\nSUMMARY:Inner\r\nEND:VTODO\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"),
	} {
		_, err := write("/L1/C.ics", body)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, name)
	}
	assert.Equal(t, before, task("C"), "it isn't changed")
	fake.Lock()
	assert.Len(t, fake.writes, writes, "nothing is written")
	fake.Unlock()

	// Its zones, and its alarms, which a task can have, are fine
	_, err = write("/L1/C.ics", strings.NewReader("BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nBEGIN:STANDARD\r\nDTSTART:19701025T030000\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\nBEGIN:VTODO\r\nSUMMARY:Errands\r\nDUE;TZID=Europe/Berlin:20261121T090000\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT1H\r\nEND:VALARM\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"))
	assert.NoError(t, err)
}

func Test_TaskWriteWatched(t *testing.T) {
	// What the filer creates, or updates, is sent to its watches as it's
	// written, without waiting to refresh
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "A", Title: "Shopping"})
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}, "refresh_interval": {"1h"}})
	ctx, cancel := context.WithCancel(t.Context())
	events, done := make(chan schema.Event, 16), make(chan error, 1)
	go func() { done <- filer.WatchObjects(ctx, events) }()
	t.Cleanup(func() { cancel(); <-done })
	next := func() string {
		t.Helper()
		select {
		case event := <-events:
			return event.EventType.String() + " " + event.Path
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
			return ""
		}
	}
	require.Equal(t, "rescan /", next())
	_, err := filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/~new.ics"}, Body: vtodo("SUMMARY:New")})
	require.NoError(t, err)
	assert.Equal(t, "created /L1/N2.ics", next())
	_, err = filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}, Body: vtodo("SUMMARY:Shopping, again")})
	require.NoError(t, err)
	assert.Equal(t, "updated /L1/A.ics", next())

	// A task whose fields are changed, but which can't be moved, is an error,
	// though the watches are sent its change
	fake.Lock()
	fake.failMove = true
	fake.Unlock()
	_, err = filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}, Body: vtodo("SUMMARY:Moved", "RELATED-TO:N2")})
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	assert.Contains(t, err.Error(), "isn't moved")
	assert.Equal(t, "updated /L1/A.ics", next())
}

func Test_TaskHidden(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFake(t)
	fake.addList("L1", "Shopping")
	fake.addTask("L1", httpclient.Task{Id: "A", Title: "Milk", Status: "needsAction"})
	fake.addTask("L1", httpclient.Task{Id: "B", Title: "Bread", Status: "completed", Hidden: true})
	count := func(filer *google.TaskFiler) string {
		t.Helper()
		dir, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1"}})
		require.NoError(t, err)
		for _, m := range dir.Meta {
			if m.Key == "children-count" {
				return string(m.Value)
			}
		}
		return ""
	}

	// A task which is hidden, as it's completed, and cleared from its list,
	// isn't listed, counted, found, or written, by default
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}})
	assert.Equal(t, []string{"/L1", "/L1/A.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))
	assert.Equal(t, "1", count(filer))
	_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/B.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/B.ics"}, Body: vtodo("SUMMARY:Bread")})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/L1/B.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.Equal(t, "false", filer.URL().Query().Get("hidden"), "it's the default")

	// With hidden=true, it is, and the parameter is the volume's
	filer = newFiler(t, endpoint, url.Values{"hidden": {"true"}})
	assert.Equal(t, []string{"/L1", "/L1/A.ics", "/L1/B.ics"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))
	assert.Equal(t, "2", count(filer))
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/B.ics"}})
	assert.NoError(t, err)
	assert.Equal(t, "true", filer.URL().Query().Get("hidden"))

	// A task which is cleared, while it's watched, is deleted, as it isn't
	// listed any more
	filer = newFiler(t, endpoint, url.Values{"refresh_interval": {"20ms"}})
	watchCtx, cancel := context.WithCancel(ctx)
	events, done := make(chan schema.Event, 16), make(chan error, 1)
	go func() { done <- filer.WatchObjects(watchCtx, events) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case event := <-events:
		require.Equal(t, schema.Rescan("tasks"), event)
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}
	fake.Lock()
	fake.tasks["L1"][0].Status, fake.tasks["L1"][0].Hidden, fake.tasks["L1"][0].ETag = "completed", true, "A-2"
	fake.Unlock()
	var got []string
	for len(got) < 2 {
		select {
		case event := <-events:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for events, got %v", got)
		}
	}
	assert.Equal(t, []string{"deleted /L1/A.ics", "updated /L1"}, got)
}

func Test_TaskWriteConcurrent(t *testing.T) {
	// Writes of a task, at once, are told to a watch in the order Google
	// made them, so its last change is the task's
	fake, endpoint := newFake(t)
	fake.addList("L1", "Home")
	fake.addTask("L1", httpclient.Task{Id: "A", Title: "Shopping"})
	filer := newFiler(t, endpoint, url.Values{"readonly": {"false"}, "refresh_interval": {"1h"}})
	ctx, cancel := context.WithCancel(t.Context())
	events, done := make(chan schema.Event, 64), make(chan error, 1)
	go func() { done <- filer.WatchObjects(ctx, events) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-events:
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}, Body: vtodo("SUMMARY:Shopping " + strconv.Itoa(i))})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// The filer's object is the one the watch was told of last, which is
	// the task's entity tag
	object, err := filer.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/L1/A.ics"}})
	require.NoError(t, err)
	fake.Lock()
	etag := ""
	for _, task := range fake.tasks["L1"] {
		if task.Id == "A" {
			etag = task.ETag
		}
	}
	fake.Unlock()
	assert.Equal(t, etag, *object.ETag)
	select {
	case event := <-events:
		assert.Equal(t, "updated /L1/A.ics", event.EventType.String()+" "+event.Path)
	case <-time.After(5 * time.Second):
		t.Fatal("no update")
	}
}

// newTaskFiler returns a task filer, of the credentials, as the registry
// creates one, with them as its options
func newTaskFiler(ctx context.Context, name string, params url.Values, credentials credential.CredentialProvider) (*google.TaskFiler, error) {
	return google.NewTaskFiler(ctx, name, params, gofiler.Opts{Credentials: credentials})
}

// newContactFilerOf returns a contact filer, of the credentials, as the
// registry creates one, with them as its options
func newContactFilerOf(ctx context.Context, name string, params url.Values, credentials credential.CredentialProvider) (*google.ContactFiler, error) {
	return google.NewContactFiler(ctx, name, params, gofiler.Opts{Credentials: credentials})
}
