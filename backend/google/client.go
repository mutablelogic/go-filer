package google

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	types "github.com/mutablelogic/go-server/pkg/types"
	trace "go.opentelemetry.io/otel/trace"
	oauth2 "golang.org/x/oauth2"
	google "golang.org/x/oauth2/google"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Client struct {
	*authclient.Client
	url         *url.URL
	tracer      trace.Tracer
	tokenSource oauth2.TokenSource
}

// serviceAccountKey is used to sniff whether a credential blob is a Google
// service account key (as opposed to a user oauth2.Token) before parsing it.
type serviceAccountKey struct {
	Type string `json:"type"`
}

var _ backend.Backend = (*Client)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	driveScheme        = "googledrive"
	driveCredentialKey = "google"
	driveScope         = "https://www.googleapis.com/auth/drive"
	endpoint           = "https://www.googleapis.com/"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(opts ...client.ClientOpt) (*Client, error) {
	self, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{Client: self}, nil
}

func (c *Client) Close() error {
	return nil
}

func NewDriveBackend(ctx context.Context, tracer trace.Tracer, decryptfn backend.DecryptCredentailFunc, u *url.URL) (*Client, error) {
	if u == nil || u.Scheme != driveScheme {
		return nil, gofiler.ErrBadParameter.Withf("url with scheme %q is required", driveScheme)
	}

	name := u.Host
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid backend name: %q", name)
	}

	raw, err := decryptfn(ctx, driveCredentialKey)
	if err != nil {
		return nil, err
	}

	var probe serviceAccountKey
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid %q credential: %v", driveCredentialKey, err)
	}

	var tokenSource oauth2.TokenSource
	if probe.Type == "service_account" {
		cfg, err := google.JWTConfigFromJSON(raw, driveScope)
		if err != nil {
			return nil, gofiler.ErrBadParameter.Withf("invalid %q service account credential: %v", driveCredentialKey, err)
		}
		tokenSource = cfg.TokenSource(ctx)
	} else {
		var token oauth2.Token
		if err := json.Unmarshal(raw, &token); err != nil {
			return nil, gofiler.ErrBadParameter.Withf("invalid %q credential: %v", driveCredentialKey, err)
		}
		tokenSource = oauth2.StaticTokenSource(&token)
	}

	client, err := New(client.OptTracer(tracer))
	if err != nil {
		return nil, err
	} else {
		client.url = &url.URL{
			Scheme: driveScheme,
			Host:   name,
			Path:   "/" + strings.TrimPrefix(strings.TrimSuffix(u.Path, "/"), "/"),
		}
		client.tracer = tracer
		client.tokenSource = tokenSource
	}

	return client, nil
}
