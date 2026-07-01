package httpclient

import (
	"io"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is a credential HTTP client that wraps the base HTTP client
// and provides typed methods for interacting with the credential API.
type Client struct {
	*client.Client
}

type textPayload struct {
	io.Reader
}

func (textPayload) Method() string { return http.MethodGet }
func (textPayload) Accept() string { return types.ContentTypeJSON }
func (textPayload) Type() string   { return types.ContentTypeTextPlain }

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new credential HTTP client with the given base URL and options.
// The url parameter should point to the credential API endpoint, e.g.
// "http://localhost:8080/api/credential".
func New(url string, opts ...client.ClientOpt) (*Client, error) {
	c := new(Client)
	cl, err := client.New(append(opts, client.OptEndpoint(url))...)
	if err != nil {
		return nil, err
	}
	c.Client = cl
	return c, nil
}
