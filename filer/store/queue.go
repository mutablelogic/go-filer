package store

import (
	"context"
	"path"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// QueueStore is the queue of the work of indexing volumes, of their objects'
// state, which outlasts a restart, so it continues. It's safe to use from
// more than one goroutine, and process, as work is leased when it's claimed.
type QueueStore interface {
	// Push queues work of the volume, which is coalesced: work of the same
	// path and kind is queued once, after the later of the two, and an index
	// of a path, or a rescan, beneath a rescan of a folder which is queued
	// isn't, as the rescan does it, and a rescan of a folder replaces the
	// index and rescan work beneath it. Returns ErrNotFound when the volume
	// isn't stored.
	Push(ctx context.Context, volume string, work ...Work) error

	// Claim leases, until the lease has passed, the work of the volume, of the
	// kinds, which is due, and isn't leased, earliest first: a rescan, on its
	// own, or up to max of the other kinds
	Claim(ctx context.Context, volume string, kinds []Kind, max int, lease time.Duration) ([]Work, error)

	// Done removes the work, which is claimed, unless it's pushed again
	// since, when it's kept, so it's claimed again
	Done(ctx context.Context, volume string, work ...Work) error

	// Failed counts an attempt of the work, which is claimed, with why it
	// failed, and makes it due again after the retry
	Failed(ctx context.Context, volume string, work Work, err error, retry time.Duration) error
}

// Kind is a kind of work
type Kind string

// Work is the work of a path of a volume
type Work struct {
	Path     string    `json:"path"`
	Kind     Kind      `json:"kind"`
	After    time.Time `json:"after,omitzero"`    // When it's due, which is now when it's pushed without one
	Attempts uint      `json:"attempts,omitzero"` // How many times it failed
	Error    string    `json:"error,omitempty"`   // Why it last failed
	Leased   time.Time `json:"leased,omitzero"`   // Until when it's leased, of a claim, which Done and Failed are of
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	KindIndex  Kind = "index"  // Index an object, of the backend
	KindRescan Kind = "rescan" // Rescan a folder, and what's beneath it
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ValidateWork returns ErrBadParameter when work's path isn't absolute, and
// clean, such as "/a/b", or its kind is empty
func ValidateWork(work Work) error {
	switch {
	case !strings.HasPrefix(work.Path, "/") || path.Clean(work.Path) != work.Path:
		return gofiler.ErrBadParameter.Withf("work of a path which isn't absolute: %q", work.Path)
	case work.Kind == "":
		return gofiler.ErrBadParameter.Withf("work of %q has no kind", work.Path)
	}
	return nil
}
