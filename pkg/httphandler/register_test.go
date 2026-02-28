package httphandler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	// Packages
	httphandler "github.com/mutablelogic/go-filer/pkg/httphandler"
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	openapi "github.com/mutablelogic/go-server/pkg/openapi/schema"
)

///////////////////////////////////////////////////////////////////////////////
// MOCK ROUTER

type mockRouter struct {
	paths  []string
	retErr error
}

func (m *mockRouter) RegisterFunc(path string, handler http.HandlerFunc, middleware bool, spec *openapi.PathItem) error {
	m.paths = append(m.paths, path)
	return m.retErr
}

///////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_RegisterHandlers(t *testing.T) {
	mgr, err := manager.New(context.Background())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	router := &mockRouter{}
	if err := httphandler.RegisterHandlers(mgr, router); err != nil {
		t.Fatalf("RegisterHandlers: %v", err)
	}

	// Four routes: /{$}, "" (bare prefix, no redirect), /{name}, /{name}/{path...}
	if len(router.paths) != 4 {
		t.Errorf("expected 4 registered paths, got %d: %v", len(router.paths), router.paths)
	}
}

func Test_RegisterHandlers_routerError(t *testing.T) {
	mgr, err := manager.New(context.Background())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer mgr.Close()

	router := &mockRouter{retErr: fmt.Errorf("router error")}
	if err := httphandler.RegisterHandlers(mgr, router); err == nil {
		t.Fatal("expected error when router.RegisterFunc fails, got nil")
	}
}
