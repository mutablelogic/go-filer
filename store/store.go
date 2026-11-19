package store

import (
	"context"
	"mime"
	"path"
	"strconv"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is where volumes, and their indexes, are kept: the objects of
// volumes, by their keys, and the state of each volume's index. A volume's
// objects, and its state, are stored once it's been created. It's safe to use
// from more than one goroutine. Folders are indexed first, and their files
// later, so an object is a folder, or a file, which a bucket can have of the
// same path, as their keys differ (see Key).
type Store interface {
	VolumeStore
	QueueStore
	ScanStore

	// Get returns the object of the volume of the key, or ErrNotFound when
	// it isn't indexed
	Get(ctx context.Context, volume, key string) (*schema.Object, error)

	// Put stores the objects of the volume, replacing those of their keys,
	// or returns ErrNotFound when the volume isn't stored
	Put(ctx context.Context, volume string, objects ...*schema.Object) error

	// Delete removes the object of the volume of the key, and, of a folder's,
	// every object beneath it, which isn't an error when there's none
	Delete(ctx context.Context, volume, key string) error

	// List returns, in the order of their keys, up to the request's limit
	// of the objects of a folder, or every object beneath it when it's
	// recursive, after the key it starts after, without the folder, and of
	// its type, when it has one (see schema.MatchType)
	List(ctx context.Context, volume string, req ListRequest) ([]*schema.Object, error)

	// Count returns how many objects a listing has, of every page, without
	// the key it starts after, or its limit
	Count(ctx context.Context, volume string, req ListRequest) (uint64, error)

	// State returns the state of the volume's index, which is IndexStatusNone
	// when it's never been indexed
	State(ctx context.Context, volume string) (schema.IndexState, error)

	// SetState stores the state of the volume's index, or returns
	// ErrNotFound when the volume isn't stored
	SetState(ctx context.Context, volume string, state schema.IndexState) error

	// Revisions returns, by their keys, the revisions of the files of a
	// folder, or of every file beneath it, when it's recursive, which are
	// what the backend's listing said of them (see Revision) when they were
	// stored, so a listing which says the same of a file needn't get it
	// again. A file which is put again, or deleted, has none, until it's
	// set again.
	Revisions(ctx context.Context, volume, folder string, recursive bool) (map[string]string, error)

	// SetRevisions stores the revisions of the files, by their keys, or
	// returns ErrNotFound when the volume isn't stored
	SetRevisions(ctx context.Context, volume string, revisions map[string]string) error
}

// ListRequest is a request for the objects of a folder
type ListRequest struct {
	Parent    string // The folder's key, such as "/", or "/a/"
	Recursive bool   // Every object beneath it, rather than its own
	After     string // The key of the last object returned, or empty
	Limit     int    // The most objects returned, or zero for every one
	Type      string // Only objects of this type, its major type, or its subtype, or empty for every one
}

// MediaType returns a type of a listing, without its parameters, in lower
// case, as schema.MatchType compares it, which has a "/" when it's a media
// type, and otherwise is a major type, or a subtype, or is empty
func MediaType(t string) string {
	if t = strings.ToLower(strings.TrimSpace(t)); strings.Contains(t, "/") {
		if mt, _, err := mime.ParseMediaType(t); err == nil {
			return mt
		}
	}
	return t
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Key returns the key of an object, which is its path, or, of a folder, its
// path with a "/" after it, so the root's is "/". Keys order a folder's
// objects as S3 does, and as backends list them, so "/a.txt" is before the
// folder "/a/", which is before "/a0.txt", and a file "/a", and a folder
// "/a/", which a bucket can have, are two objects.
func Key(object *schema.Object) string {
	if object.IsDir {
		return FolderKey(object.Path)
	}
	return object.Path
}

// FolderKey returns the key of the folder of a path, which ends with "/"
func FolderKey(p string) string {
	if strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

// PathOf returns the path of a key, without the "/" after a folder's,
// unless it's the root, and whether it's a folder's
func PathOf(key string) (string, bool) {
	if key == "/" {
		return key, true
	} else if folder, ok := strings.CutSuffix(key, "/"); ok {
		return folder, true
	}
	return key, false
}

// ParentKey returns the key of the folder of a key, which is "/" for the
// root's objects, and empty for the root
func ParentKey(key string) string {
	if key == "/" || key == "" {
		return ""
	}
	p := strings.TrimSuffix(key, "/")
	return p[:strings.LastIndex(p, "/")+1]
}

// Ancestors returns the keys of the folders above a key, nearest first, so
// "/a/b.txt" is "/a/" and "/", and the root has none
func Ancestors(key string) []string {
	result := []string{}
	for parent := ParentKey(key); parent != ""; parent = ParentKey(parent) {
		result = append(result, parent)
	}
	return result
}

// Subtree returns the range of the keys beneath a folder's key, which are
// after it, and before the upper, its path with the byte after "/", which
// is "0", so "/a/" is to "/a0", which "/ab" isn't in, and "/" is to "0"
func Subtree(folder string) (string, string) {
	return folder, strings.TrimSuffix(folder, "/") + "0"
}

// ValidateFolderKey returns ErrBadParameter when a key isn't valid (see
// ValidateKey), or isn't a folder's, which ends with "/"
func ValidateFolderKey(key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	} else if _, folder := PathOf(key); !folder {
		return gofiler.ErrBadParameter.Withf("%q isn't a folder's key", key)
	}
	return nil
}

// Revision returns the revision of an object, of a listing: its entity tag,
// size and modification time, which a listing changes when any of them do,
// as a bucket's does when only an object's metadata is written, whose
// entity tag is the same, and whose time is when it was written
func Revision(object *schema.Object) string {
	etag, size := "", ""
	if object.ETag != nil {
		etag = *object.ETag
	}
	if object.Size != nil {
		size = strconv.FormatInt(*object.Size, 10)
	}
	return etag + " " + size + " " + object.ModTime.UTC().Format(time.RFC3339Nano)
}

// ValidateKey returns ErrBadParameter when a key isn't absolute, and clean,
// such as "/a/b.txt", or a folder's, "/a/", or the root, "/"
func ValidateKey(key string) error {
	if p, _ := PathOf(key); !strings.HasPrefix(key, "/") || strings.Contains(key, "//") || path.Clean(p) != p {
		return gofiler.ErrBadParameter.Withf("a key which isn't absolute, and clean: %q", key)
	}
	return nil
}
