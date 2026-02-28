package httpclient

import (
	// Packages
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is a filer HTTP client that wraps the base HTTP client
// and provides typed methods for interacting with the filer API.
type Client struct {
	*client.Client
}

///////////////////////////////////////////////////////////////////////////////
// CONSTANTS

// parallelHeads is the maximum number of concurrent HEAD requests issued by GetObjects.
const parallelHeads = 10

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new filer HTTP client with the given base URL and options.
// The url parameter should point to the filer API endpoint, e.g.
// "http://localhost:8080/api/filer".
func New(url string, opts ...client.ClientOpt) (*Client, error) {
	c := new(Client)
	cl, err := client.New(append(opts, client.OptEndpoint(url))...)
	if err != nil {
		return nil, err
	}
	c.Client = cl
	return c, nil
}
