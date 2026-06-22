package gofiler

import (
	"errors"
	"testing"

	pg "github.com/mutablelogic/go-pg"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

func TestHTTPErrMapsPgNotFound(t *testing.T) {
	err := HTTPErr(pg.ErrNotFound)
	if !errors.Is(err, httpresponse.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
