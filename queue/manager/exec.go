package manager

import (
	"strings"
	// Packages
	"sync"

	schema "github.com/mutablelogic/go-filer/queue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type exec struct {
	sync.RWMutex
	t map[string]schema.TaskFunc
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterTask stores a named task callback. Names are normalized to lowercase
// identifiers and must be unique.
func (exec *exec) RegisterTask(name string, fn schema.TaskFunc) error {
	name, err := taskName(name)
	if err != nil {
		return err
	}
	if fn == nil {
		return httpresponse.ErrBadRequest.With("missing task callback")
	}

	exec.Lock()
	defer exec.Unlock()

	if exec.t == nil {
		exec.t = make(map[string]schema.TaskFunc)
	}
	if _, exists := exec.t[name]; exists {
		return httpresponse.ErrConflict.Withf("task %q already registered", name)
	}
	exec.t[name] = fn

	return nil
}

// RemoveTask removes a named task callback.
func (exec *exec) RemoveTask(name string) error {
	name, err := taskName(name)
	if err != nil {
		return err
	}

	exec.Lock()
	defer exec.Unlock()

	if exec.t == nil {
		return httpresponse.ErrNotFound.Withf("task %q not found", name)
	}
	if _, exists := exec.t[name]; !exists {
		return httpresponse.ErrNotFound.Withf("task %q not found", name)
	}
	delete(exec.t, name)

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func taskName(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", httpresponse.ErrBadRequest.With("missing task name")
	}
	if !types.IsIdentifier(name) {
		return "", httpresponse.ErrBadRequest.Withf("invalid task name: %q", name)
	}
	return name, nil
}
