package httpclient

import (
	"crypto/tls"
	"net/http"
	"os"
	"strings"

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
	if isTruthyEnv("FILER_HTTP1") {
		if tr, ok := cl.Client.Transport.(*http.Transport); ok && tr != nil {
			tr = tr.Clone()
			tr.ForceAttemptHTTP2 = false
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
			cl.Client.Transport = tr
		} else {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			tr.ForceAttemptHTTP2 = false
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
			cl.Client.Transport = tr
		}
	}
	c.Client = cl
	return c, nil
}

func isTruthyEnv(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v != "" && v != "0" && v != "false" && v != "no" && v != "off"
}
