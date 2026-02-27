package httpclient

import (
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is a filer HTTP client that wraps the base HTTP client
// and provides typed methods for interacting with the filer API.
type Client struct {
	*client.Client
	baseURL string // stored for streaming requests that bypass go-client
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new filer HTTP client with the given base URL and options.
// The url parameter should point to the filer API endpoint, e.g.
// "http://localhost:8080/api/filer".
func New(url string, opts ...client.ClientOpt) (*Client, error) {
	c := new(Client)
	c.baseURL = url
	if client, err := client.New(append(opts, client.OptEndpoint(url))...); err != nil {
		return nil, err
	} else {
		c.Client = client
	}
	return c, nil
}

// httpClient returns a plain *http.Client for streaming requests.
func (c *Client) httpClient() *http.Client {
	return &http.Client{}
}
