package store

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ScanStore keeps what a scan of a folder of a volume has seen, of the
// backend's listing, so a volume's objects aren't held in memory while it's
// scanned, which the scan sweeps the index of, and compares the index's files
// with. A volume is scanned one folder at a time, by one worker, so what's
// seen beneath a folder is of its scan: the indexer of a volume is its only
// worker, as a volume is indexed by one process. A store which is shared by
// processes, such as Postgres, needs a lock of a volume's scans (#72).
type ScanStore interface {
	// Seen records the keys which a scan has seen, with the revisions of
	// their listings (see Revision), which a folder's is empty, or returns
	// ErrNotFound when the volume isn't stored
	Seen(ctx context.Context, volume string, seen map[string]string) error

	// Unsee forgets what a scan has seen beneath the folder's key
	Unsee(ctx context.Context, volume, folder string) error

	// Sweep removes the objects beneath the folder's key, with their
	// metadata, which a scan of it hasn't seen
	Sweep(ctx context.Context, volume, folder string) error

	// Changed returns, in the order of their keys, after the key, up to the
	// limit, the files beneath the folder's key which a scan of it has seen,
	// which aren't indexed, or whose revisions aren't those they were
	// stored of (see Revisions), so they're got again
	Changed(ctx context.Context, volume, folder, after string, limit int) ([]Seen, error)

	// CountChanged returns how many files Changed returns, of every page
	CountChanged(ctx context.Context, volume, folder string) (uint64, error)
}

// Seen is a file which a scan has seen, which has changed, with the revision
// of its listing, and whether it's indexed
type Seen struct {
	Key      string
	Revision string
	Indexed  bool
}
