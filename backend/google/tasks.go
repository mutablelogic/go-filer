// Package google has filers of Google's services, such as Tasks, whose
// requests are with the clients of package httpclient, so a request which
// isn't authorized returns an AuthError, which says a sign-in is needed.
package google

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// TaskFiler is a filer of Google Tasks: each task list is a directory, of
// its id, and each task a file, of its id, as an iCalendar of its VTODO,
// with its metadata, such as its due date
type TaskFiler struct {
	taskOpt
	name   string
	opts   gofiler.Opts // Its credentials, and client options, which a clone is created with
	tokens *tokens
	client *httpclient.TasksClient

	// The watches, which are sent the changes the filer makes, so they're
	// sent them without waiting to refresh
	// writes serialises the filer's writes, and deletes, through telling the
	// watches, so they're told in the order Google made them
	writes sync.Mutex

	mu      sync.Mutex
	watches map[*taskWatch]struct{}
	seq     uint64 // The sequence of the last change the filer made
}

// taskOpt are the options of a task filer, of its parameters
type taskOpt struct {
	readonly        bool          // Tasks can't be created, updated or deleted
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How often changes are polled for, or zero not to
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in, which is DefaultCredential, unless it's set
	hidden          bool          // Tasks which are hidden, as they're completed, and cleared, are included
	description     string        // What the volume is, which is of the user who signed in, by default
}

var _ gofiler.Filer = (*TaskFiler)(nil)

// tasksEndpoint is the endpoint of the API, which is Google's, unless a
// test sets another. It isn't a parameter, as a volume's URL could send the
// sign-in's token to another server.
var tasksEndpoint = httpclient.TasksEndpoint

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// TasksScheme is the scheme of a task filer's URL, such as
	// gtasks://tasks?readonly=false
	TasksScheme = "gtasks"

	// DefaultTasksQuota is the most requests each minute, unless the quota
	// parameter sets another
	DefaultTasksQuota = 120

	// DefaultRefreshInterval is how often changes are polled for, unless the
	// refresh_interval parameter sets another
	DefaultRefreshInterval = time.Minute

	// The parameters of a task filer
	paramReadonly        = "readonly"
	paramQuota           = "quota"
	paramRefreshInterval = "refresh_interval"
	paramCredential      = "credential"
	paramHidden          = "hidden"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewTaskFiler returns a filer of the Google Tasks of the volume's name,
// with the parameters, the user's sign-in, of the provider of credentials,
// and go-client's options of its requests, such as a tracer. The sign-in is
// checked, and the tasks, so a sign-in which is needed is an
// SignInError, with how to sign in, which is when there isn't one, or
// it's for another client than ClientCredential's, or it doesn't have the
// access the volume needs. The parameters are:
//
//   - readonly: tasks can't be created, updated or deleted, which is true
//     unless it's false, which needs the tasks scope, rather than
//     tasks.readonly
//   - credential: the key of the sign-in, which is DefaultCredential
//   - quota: the most requests each minute, which is DefaultTasksQuota, or
//     zero not to limit them
//   - refresh_interval: how often changes are polled for, when they're
//     watched, which is DefaultRefreshInterval, or zero not to watch them
//   - hidden: tasks which are hidden, as they're completed, and cleared from
//     their list, are included, which they aren't unless it's true
func NewTaskFiler(ctx context.Context, name string, params url.Values, opts gofiler.Opts) (*TaskFiler, error) {
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseTaskOpts(params)
	if err != nil {
		return nil, err
	}
	opts.ClientOpts = slices.Clone(opts.ClientOpts)
	self := &TaskFiler{taskOpt: opt, name: name, opts: opts}

	// The user's sign-in, which has the access the volume needs
	required, scopes := []string{httpclient.TasksScope}, []string{httpclient.TasksScope, httpclient.EmailScope}
	if opt.readonly {
		required, scopes = []string{httpclient.TasksReadonlyScope, httpclient.TasksScope}, []string{httpclient.TasksReadonlyScope, httpclient.EmailScope}
	}
	if self.tokens, err = newTokens(ctx, opts.Credentials, opt.credential, required, scopes); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	clientopts := append(slices.Clone(opts.ClientOpts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		clientopts = append(clientopts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewTasksClient(opt.endpoint, clientopts...); err != nil {
		return nil, err
	}

	// Check the tasks can be read
	if err := self.client.Check(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}

	// What the volume is, when it isn't set, is of the user who signed in
	if self.description == "" {
		self.description = describe(ctx, "Google Tasks", self.tokens, opts.ClientOpts)
	}
	return self, nil
}

// parseTaskOpts returns the options of the parameters, which are rejected
// when they can't be parsed
func parseTaskOpts(params url.Values) (taskOpt, error) {
	opt := taskOpt{readonly: true, quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: tasksEndpoint}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential, paramHidden, schema.OptDescription:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
	opt.description = params.Get(schema.OptDescription)
	if value := params.Get(paramReadonly); value != "" {
		readonly, err := strconv.ParseBool(value)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramReadonly, value)
		}
		opt.readonly = readonly
	}
	if value := params.Get(paramQuota); value != "" {
		quota, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramQuota, value)
		}
		opt.quota = uint(quota)
	}
	if value := params.Get(paramRefreshInterval); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < 0 {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramRefreshInterval, value)
		}
		opt.refreshInterval = interval
	}
	if value := params.Get(paramCredential); value != "" {
		if err := credentialKey(value); err != nil {
			return opt, err
		}
		opt.credential = value
	}
	if value := params.Get(paramHidden); value != "" {
		hidden, err := strconv.ParseBool(value)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramHidden, value)
		}
		opt.hidden = hidden
	}
	return opt, nil
}

// Close the filer
func (self *TaskFiler) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// URL returns the URL of the filer, such as gtasks://tasks, with each of
// its parameters
func (self *TaskFiler) URL() *url.URL {
	return &url.URL{Scheme: TasksScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the tasks, with the parameters changed by
// the patch, and the same client options, which is checked as a new filer
// is
func (self *TaskFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	// A clone of another sign-in, without a description, is described of it
	params := gofiler.PatchValues(self.params(), patch)
	credential := params.Get(paramCredential)
	if credential == "" {
		credential = DefaultCredential
	}
	if credential != self.credential && !patch.Has(schema.OptDescription) {
		params.Del(schema.OptDescription)
	}
	clone, err := NewTaskFiler(ctx, self.name, params, self.opts)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

// Readonly returns true when tasks can't be created, updated or deleted
func (self *TaskFiler) Readonly() bool {
	return self.readonly
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - WRITE

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns the parameters of the filer, each of which is set, so the
// URL says how it's set up, defaults too
func (self *TaskFiler) params() url.Values {
	return url.Values{
		paramReadonly:         {strconv.FormatBool(self.readonly)},
		paramQuota:            {strconv.FormatUint(uint64(self.quota), 10)},
		paramRefreshInterval:  {self.refreshInterval.String()},
		paramCredential:       {self.credential},
		paramHidden:           {strconv.FormatBool(self.hidden)},
		schema.OptDescription: {self.description},
	}
}

// apiError returns the error of a request, of the operation, of the path:
// a SignInError, as it is, or of Google's challenge, when the token
// isn't accepted, such as when it's revoked, so the user signs in again,
// fs.ErrNotExist when it isn't found, a context's error, as it is, or else
// ErrServiceUnavailable
func (self *TaskFiler) apiError(op, path string, err error) error {
	return apiError(self.tokens, self.credential, op, path, err)
}

// apiError returns the error of a request of a backend of Google's, of its
// sign-in, and the key of its credential, as TaskFiler's apiError does
func apiError(tokens *tokens, credential, op, path string, err error) error {
	var authErr *authclient.AuthError
	switch {
	case err == nil:
		return nil
	case authclient.SignInOf(err) != nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &authErr):
		return tokens.signInAgain(gofiler.ErrNotAuthorized.Withf("Google didn't accept sign-in %q, so sign in with Google again: %v", credential, err))
	case errors.Is(err, httpresponse.Err(http.StatusNotFound)):
		return &fs.PathError{Op: op, Path: path, Err: fs.ErrNotExist}
	case errors.Is(err, httpresponse.Err(http.StatusForbidden)):
		return gofiler.ErrForbidden.Withf("%s %s: %v", op, path, err)
	case errors.Is(err, httpresponse.Err(http.StatusBadRequest)):
		// Such as a parent which isn't a task of the list
		return gofiler.ErrBadParameter.Withf("%s %s: %v", op, path, err)
	default:
		return gofiler.ErrServiceUnavailable.Withf("%s %s: %v", op, path, fmt.Sprint(err))
	}
}

////////////////////////////////////////////////////////////////////////////////
// TRANSPORT

// tokenTransport sets the token of each request to the API, of the
// sign-in, which is refreshed, when it has expired. A request to another
// origin, such as a redirect, isn't sent it.
type tokenTransport struct {
	parent       http.RoundTripper
	tokens       *tokens
	scheme, host string // The origin of the API
}

func (t *tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	parent := t.parent
	if parent == nil {
		parent = http.DefaultTransport
	}
	if req.URL.Scheme != t.scheme || req.URL.Host != t.host {
		return parent.RoundTrip(req)
	}
	token, err := t.tokens.token(req.Context())
	if err != nil {
		return nil, err
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)
	return parent.RoundTrip(req)
}
