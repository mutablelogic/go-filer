package indexer

import (
	"context"
	"path"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is where an index is kept: the objects of volumes, by their paths,
// and the state of each volume's index. It's safe to use from more than one
// goroutine. Folders are indexed first, and their files later, so an object
// is a folder, or a file.
type Store interface {
	// Get returns the object of the volume at the path, or ErrNotFound
	// when it isn't indexed
	Get(ctx context.Context, volume, path string) (*schema.Object, error)

	// Put stores the objects of the volume, replacing those at their paths
	Put(ctx context.Context, volume string, objects ...*schema.Object) error

	// Delete removes the object of the volume at the path, and every object
	// beneath it, which isn't an error when there's none
	Delete(ctx context.Context, volume, path string) error

	// List returns, in the order of their keys, up to the request's limit
	// of the objects of a folder, or every object beneath it when it's
	// recursive, after the key it starts after, without the folder
	List(ctx context.Context, volume string, req ListRequest) ([]*schema.Object, error)

	// State returns the state of the volume's index, which is StatusNone
	// when it's never been indexed
	State(ctx context.Context, volume string) (State, error)

	// SetState stores the state of the volume's index
	SetState(ctx context.Context, volume string, state State) error
}

// ListRequest is a request for the objects of a folder
type ListRequest struct {
	Parent    string // The folder's path, such as "/", or "/a"
	Recursive bool   // Every object beneath it, rather than its own
	After     string // The Key of the last object returned, or empty
	Limit     int    // The most objects returned, or zero for every one
}

// State is the state of a volume's index
type State struct {
	Status  Status    `json:"status"`
	Folders uint64    `json:"folders,omitempty"` // How many folders the scan in progress, or the last, has indexed
	Started time.Time `json:"started,omitzero"`  // When the scan in progress, or the last, started
	Scanned time.Time `json:"scanned,omitzero"`  // When the last scan which finished did
	Error   string    `json:"error,omitempty"`   // Why the last scan, or the watch, failed
}

// Status is how far a volume is indexed
type Status string

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	StatusNone     Status = ""         // It's never been indexed
	StatusScanning Status = "scanning" // It's being scanned, for the first time, or again
	StatusReady    Status = "ready"    // It's indexed, and the index follows its changes
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Key returns the key of an object, which orders a folder's objects as S3
// does, and as backends list them: a folder's path with a "/" after it, so
// "/a.txt" is before the folder "/a", which is before "/a0.txt"
func Key(object *schema.Object) string {
	if object.IsDir && object.Path != "/" {
		return object.Path + "/"
	}
	return object.Path
}

// Parent returns the path of the folder of a path, which is "/" for the
// root's objects, and empty for the root
func Parent(p string) string {
	if p == "/" || p == "" {
		return ""
	}
	return path.Dir(p)
}

// Beneath returns true when a path is beneath the folder, which is every
// path but the root's own, for the root
func Beneath(folder, p string) bool {
	if folder == "/" {
		return p != "/" && strings.HasPrefix(p, "/")
	}
	return strings.HasPrefix(p, folder+"/")
}
