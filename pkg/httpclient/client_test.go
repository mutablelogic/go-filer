package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/pkg/httpclient"
	httphandler "github.com/mutablelogic/go-filer/pkg/httphandler"
	manager "github.com/mutablelogic/go-filer/pkg/manager"
)

func newTestServer(t *testing.T, backends ...string) (*httpclient.Client, func()) {
	t.Helper()
	opts := make([]manager.Opt, 0, len(backends))
	for _, b := range backends {
		opts = append(opts, manager.WithBackend(context.Background(), b))
	}
	mgr, err := manager.New(context.Background(), opts...)
	if err != nil {
		t.Fatalf("newTestServer: failed to create manager: %v", err)
	}
	mux := http.NewServeMux()
	p, h, _ := httphandler.BackendListHandler(mgr)
	mux.HandleFunc(p, h)
	p, h, _ = httphandler.ObjectListHandler(mgr)
	mux.HandleFunc(p, h)
	p, h, _ = httphandler.ObjectHandler(mgr)
	mux.HandleFunc(p, h)
	srv := httptest.NewServer(mux)
	c, err := httpclient.New(srv.URL)
	if err != nil {
		srv.Close()
		mgr.Close()
		t.Fatalf("newTestServer: failed to create client: %v", err)
	}
	return c, func() {
		srv.Close()
		mgr.Close()
	}
}
