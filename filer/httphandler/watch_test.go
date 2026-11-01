package httphandler_test

import (
	"net/http"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestWatchAccept(t *testing.T) {
	server, _ := newServer(t)

	// Changes are only streamed, so another type isn't acceptable
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/watch", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}
