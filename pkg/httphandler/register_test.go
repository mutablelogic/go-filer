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

	// Four routes: /{$} and "" (bare prefix, prevents ServeMux redirect),
	// /{name} (backend object listing), /{name}/{path...} (object operations)
	expected := []string{"/{$}", "", "/{name}", "/{name}/{path...}"}
	if len(router.paths) != len(expected) {
		t.Fatalf("expected %d registered paths, got %d: %v", len(expected), len(router.paths), router.paths)
	}
	for i, want := range expected {
		if router.paths[i] != want {
			t.Errorf("path[%d]: want %q, got %q", i, want, router.paths[i])
		}
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
