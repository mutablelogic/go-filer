package store

import (
	"context"
	"path"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is where volumes, and their indexes, are kept: the objects of
// volumes, by their paths, and the state of each volume's index. A volume's
// objects, and its state, are stored once it's been created. It's safe to use
// from more than one goroutine. Folders are indexed first, and their files
// later, so an object is a folder, or a file.
type Store interface {
	VolumeStore
	QueueStore

	// Get returns the object of the volume at the path, or ErrNotFound
	// when it isn't indexed
	Get(ctx context.Context, volume, path string) (*schema.Object, error)

	// Put stores the objects of the volume, replacing those at their paths,
	// or returns ErrNotFound when the volume isn't stored
	Put(ctx context.Context, volume string, objects ...*schema.Object) error

	// Delete removes the object of the volume at the path, and every object
	// beneath it, which isn't an error when there's none
	Delete(ctx context.Context, volume, path string) error

	// List returns, in the order of their keys, up to the request's limit
	// of the objects of a folder, or every object beneath it when it's
	// recursive, after the key it starts after, without the folder
	List(ctx context.Context, volume string, req ListRequest) ([]*schema.Object, error)

	// State returns the state of the volume's index, which is IndexStatusNone
	// when it's never been indexed
	State(ctx context.Context, volume string) (schema.IndexState, error)

	// SetState stores the state of the volume's index, or returns
	// ErrNotFound when the volume isn't stored
	SetState(ctx context.Context, volume string, state schema.IndexState) error
}

// ListRequest is a request for the objects of a folder
type ListRequest struct {
	Parent    string // The folder's path, such as "/", or "/a"
	Recursive bool   // Every object beneath it, rather than its own
	After     string // The Key of the last object returned, or empty
	Limit     int    // The most objects returned, or zero for every one
}

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
