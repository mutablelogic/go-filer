package schema

import (
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ScanSeen is a key of a volume which a scan has seen, with the revision of
// its listing, which is put
type ScanSeen struct {
	Volume   string
	Key      string
	Revision string
	Now      time.Time
}

// ScanFolder selects what a scan of a folder of a volume has seen, which is
// forgotten, swept, or counted, of its changed files
type ScanFolder struct {
	Volume string
	Folder string
}

// ScanChanged selects the files which a scan of a folder of a volume has
// seen, which have changed, after the key, up to the limit
type ScanChanged struct {
	Volume string
	Folder string
	After  string
	Limit  int
}

// ScanChangedList is the files which are read, which have changed
type ScanChangedList []store.Seen

// ScanCount is how many files have changed
type ScanCount uint64

// ScanUnsee, and ScanSweep, are the deletions of a scan's folder
type ScanUnsee ScanFolder
type ScanSweep ScanFolder

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds the key, and its revision, which replaces the one it has
func (seen ScanSeen) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", seen.Volume)
	bind.Set("key", seen.Key)
	bind.Set("revision", seen.Revision)
	bind.Set("now", seen.Now.UTC())
	return bind.Query("scan.seen"), nil
}

func (seen ScanSeen) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("what a scan has seen is put")
}

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (unsee ScanUnsee) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Delete {
		return "", sqlite.ErrNotImplemented.Withf("scan unsee: %v", op)
	}
	bindFolder(bind, ScanFolder(unsee))
	return bind.Query("scan.unsee"), nil
}

func (sweep ScanSweep) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Delete {
		return "", sqlite.ErrNotImplemented.Withf("scan sweep: %v", op)
	}
	bindFolder(bind, ScanFolder(sweep))
	return bind.Query("scan.sweep"), nil
}

func (folder ScanFolder) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Get {
		return "", sqlite.ErrNotImplemented.Withf("scan count: %v", op)
	}
	bindFolder(bind, folder)
	return bind.Query("scan.count_changed"), nil
}

func (changed ScanChanged) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("scan changed: %v", op)
	}
	bindFolder(bind, ScanFolder{Volume: changed.Volume, Folder: changed.Folder})
	bind.Set("after", changed.After)
	if changed.Limit > 0 {
		bind.Set("offsetlimit", "LIMIT "+bind.Set("limit", changed.Limit))
	}
	return bind.Query("scan.changed"), nil
}

// bindFolder binds the volume, and the range of the keys beneath the folder
func bindFolder(bind *sqlite.Bind, folder ScanFolder) {
	bind.Set("volume", folder.Volume)
	lower, upper := store.Subtree(folder.Folder)
	bind.Set("lower", lower)
	bind.Set("upper", upper)
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (list *ScanChangedList) Scan(row sqlite.Row) error {
	var seen store.Seen
	if err := row.Scan(&seen.Key, &seen.Revision, &seen.Indexed); err != nil {
		return err
	}
	*list = append(*list, seen)
	return nil
}

func (count *ScanCount) Scan(row sqlite.Row) error {
	return row.Scan((*uint64)(count))
}
