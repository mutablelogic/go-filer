package store

import (
	"context"
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
	// key and kind is queued once, due at the earlier of the two, so work
	// which is pushed again and again isn't put off, and an index of a key,
	// or a rescan, beneath a rescan of a folder which is queued isn't, as
	// the rescan does it, and a rescan of a folder replaces the index and
	// rescan work beneath it. A rescan of the root is a catch-up, of changes
	// which can have been missed, which doesn't hold up reads (see Pending),
	// so it replaces only the rescans beneath it, and an index isn't of it.
	// Returns ErrNotFound when the volume isn't stored.
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

	// Pending returns true when the key has work queued, or claimed, which
	// would change its index: an index, or a rescan, of it, or of a folder
	// above it, so the index of it can be out of date, but not a rescan of
	// the root, which is a catch-up, which the index is served while it's
	// done
	Pending(ctx context.Context, volume, key string) (bool, error)

	// PendingBeneath returns true when the folder's key has work queued, or
	// claimed, which would change its listing: work which Pending is true
	// of, or an index, or a rescan, of a key beneath it, so the index of
	// what's beneath it can be out of date
	PendingBeneath(ctx context.Context, volume, folder string) (bool, error)
}

// Kind is a kind of work
type Kind string

// Work is the work of a key of a volume: of a file, or of a folder, whose
// key ends with "/"
type Work struct {
	Key      string    `json:"key"`
	Kind     Kind      `json:"kind"`
	After    time.Time `json:"after,omitzero"`    // When it's due, which is now when it's pushed without one
	Attempts uint      `json:"attempts,omitzero"` // How many times it failed
	Error    string    `json:"error,omitempty"`   // Why it last failed
	Leased   time.Time `json:"leased,omitzero"`   // Until when it's leased, of a claim, which Done and Failed are of
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	KindIndex  Kind = "index"  // Index an object, a file, or a folder, of the backend
	KindRescan Kind = "rescan" // Rescan a folder, and what's beneath it, of its key
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ValidateWork returns ErrBadParameter when work's key isn't valid (see
// ValidateKey), its kind is empty, or it's a rescan of a key which isn't a
// folder's
func ValidateWork(work Work) error {
	if err := ValidateKey(work.Key); err != nil {
		return err
	} else if work.Kind == "" {
		return gofiler.ErrBadParameter.Withf("work of %q has no kind", work.Key)
	} else if work.Kind == KindRescan {
		return ValidateFolderKey(work.Key)
	}
	return nil
}
