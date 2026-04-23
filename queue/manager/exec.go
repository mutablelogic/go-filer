package manager

import (
	"context"
	"strings"
	"time"

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

// RunQueueTask executes a named task callback with the given payload.
func (exec *exec) RunTickerTask(ctx context.Context, ticker *schema.Ticker) error {
	// Create a deadline for the task execution based on the ticker's period
	// and the current time. This ensures that the task will not run indefinitely
	// and will be cancelled if it exceeds the ticker's period.
	deadline := time.Now().Add(time.Minute)
	if interval := types.Value(ticker.Interval); interval > 0 {
		deadline = time.Now().Add(interval)
	}

	// Create the context
	child, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// TODO: Add the ticker into the context

	// Get the task function for the ticker's name
	exec.RLock()
	defer exec.RUnlock()
	if fn, exists := exec.t[ticker.Ticker]; exists {
		// Run the task function with the provided payload and deadline
		// TODO: Make a span for the ticker
		_, err := fn(child, ticker.Payload)
		if err != nil {
			// TODO: Emit the error with the span
			return err
		}
	} else {
		return httpresponse.ErrNotFound.Withf("task callback %q not found", ticker.Ticker)
	}

	// Return success
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func taskName(name string) (string, error) {
	if name = strings.ToLower(strings.TrimSpace(name)); !types.IsIdentifier(name) {
		return "", httpresponse.ErrBadRequest.Withf("invalid task name: %q", name)
	} else {
		return name, nil
	}
}
