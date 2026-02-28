package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	// Packages
	client "github.com/mutablelogic/go-client"
	httpclient "github.com/mutablelogic/go-filer/pkg/httpclient"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Client builds a filer HTTP client from the global HTTP flags.
func (g *Globals) Client() (*httpclient.Client, error) {
	endpoint, err := g.clientEndpoint()
	if err != nil {
		return nil, err
	}
	opts := []client.ClientOpt{}
	if g.Debug {
		opts = append(opts, client.OptTrace(os.Stderr, false))
	}
	if g.HTTP.Timeout > 0 {
		opts = append(opts, client.OptTimeout(g.HTTP.Timeout))
	}
	return httpclient.New(endpoint, opts...)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (g *Globals) clientEndpoint() (string, error) {
	scheme := "http"
	host, port, err := net.SplitHostPort(g.HTTP.Addr)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "localhost"
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	portn, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", err
	}
	if portn == 443 {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:%v%s", scheme, host, portn, types.NormalisePath(g.HTTP.Prefix)), nil
}
