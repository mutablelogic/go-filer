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
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
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
	name        string
	credentials gofiler.CredentialProvider
	tokens      *tokens
	clientopts  []client.ClientOpt
	client      *httpclient.TasksClient
}

// taskOpt are the options of a task filer, of its parameters
type taskOpt struct {
	readonly        bool          // Tasks can't be created, updated or deleted
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How often changes are polled for, or zero not to
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in, which is DefaultCredential, unless it's set
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
func NewTaskFiler(ctx context.Context, name string, params url.Values, credentials gofiler.CredentialProvider, clientopts ...client.ClientOpt) (*TaskFiler, error) {
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseTaskOpts(params)
	if err != nil {
		return nil, err
	}
	self := &TaskFiler{taskOpt: opt, name: name, credentials: credentials, clientopts: slices.Clone(clientopts)}

	// The user's sign-in, which has the access the volume needs
	required, scopes := []string{httpclient.TasksScope}, []string{httpclient.TasksScope}
	if opt.readonly {
		required, scopes = []string{httpclient.TasksReadonlyScope, httpclient.TasksScope}, []string{httpclient.TasksReadonlyScope}
	}
	if self.tokens, err = newTokens(ctx, credentials, opt.credential, required, scopes); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	opts := append(slices.Clone(clientopts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		opts = append(opts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewTasksClient(opt.endpoint, opts...); err != nil {
		return nil, err
	}

	// Check the tasks can be read
	if err := self.client.Check(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}
	return self, nil
}

// parseTaskOpts returns the options of the parameters, which are rejected
// when they can't be parsed
func parseTaskOpts(params url.Values) (taskOpt, error) {
	opt := taskOpt{readonly: true, quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: tasksEndpoint}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
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
	return opt, nil
}

// Close the filer
func (self *TaskFiler) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// URL returns the URL of the filer, such as gtasks://tasks, with the
// parameters which aren't their defaults
func (self *TaskFiler) URL() *url.URL {
	return &url.URL{Scheme: TasksScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the tasks, with the parameters changed by
// the patch, and the same client options, which is checked as a new filer
// is
func (self *TaskFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	clone, err := NewTaskFiler(ctx, self.name, gofiler.PatchValues(self.params(), patch), self.credentials, self.clientopts...)
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

// CreateObject creates, or updates, a task, which isn't implemented yet
func (self *TaskFiler) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	return nil, gofiler.ErrNotImplemented.With("writing tasks isn't implemented yet")
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns the parameters of the filer which aren't their defaults
func (self *TaskFiler) params() url.Values {
	params := url.Values{}
	if !self.readonly {
		params.Set(paramReadonly, "false")
	}
	if self.quota != DefaultTasksQuota {
		params.Set(paramQuota, strconv.FormatUint(uint64(self.quota), 10))
	}
	if self.refreshInterval != DefaultRefreshInterval {
		params.Set(paramRefreshInterval, self.refreshInterval.String())
	}
	if self.credential != DefaultCredential {
		params.Set(paramCredential, self.credential)
	}
	return params
}

// apiError returns the error of a request, of the operation, of the path:
// a SignInError, as it is, or of Google's challenge, when the token
// isn't accepted, such as when it's revoked, so the user signs in again,
// fs.ErrNotExist when it isn't found, a context's error, as it is, or else
// ErrServiceUnavailable
func (self *TaskFiler) apiError(op, path string, err error) error {
	var authErr *authclient.AuthError
	switch {
	case err == nil:
		return nil
	case authclient.SignInOf(err) != nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &authErr):
		return self.tokens.signInAgain(gofiler.ErrNotAuthorized.Withf("Google didn't accept sign-in %q, so sign in with Google again: %v", self.credential, err))
	case errors.Is(err, httpresponse.Err(http.StatusNotFound)):
		return &fs.PathError{Op: op, Path: path, Err: fs.ErrNotExist}
	case errors.Is(err, httpresponse.Err(http.StatusForbidden)):
		return gofiler.ErrForbidden.Withf("%s %s: %v", op, path, err)
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
