package sqlite

import (
	"context"
	"errors"
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	schema "github.com/mutablelogic/go-filer/filer/store/sqlite/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Push queues work of the volume, coalesced, in one transaction, or returns
// ErrNotFound when the volume isn't stored
func (self *Store) Push(ctx context.Context, volume string, work ...store.Work) (err error) {
	ctx, end := self.span(ctx, "Push", attribute.String("arg.volume", volume), attribute.Int("arg.work", len(work)))
	defer func() { end(err) }()
	now := time.Now()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var exists schema.VolumeList
		if err := conn.Get(ctx, &exists, schema.VolumeName(volume)); err != nil {
			return volumeErr(err, volume)
		}
		for _, w := range work {
			if err := store.ValidateWork(w); err != nil {
				return err
			}
			if w.After.IsZero() {
				w.After = now
			}
			if err := conn.Insert(ctx, nil, schema.QueuePush{Volume: volume, Work: w}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// Claim leases the work of the volume, of the kinds, which is due, and isn't
// leased, earliest first: a rescan, on its own, or up to max of the other
// kinds
func (self *Store) Claim(ctx context.Context, volume string, kinds []store.Kind, max int, lease time.Duration) (_ []store.Work, err error) {
	ctx, end := self.span(ctx, "Claim", attribute.String("arg.volume", volume), attribute.Int("arg.max", max))
	defer func() { end(err) }()
	if max <= 0 || len(kinds) == 0 {
		return nil, nil
	}
	now := time.Now()
	var result schema.WorkList
	if err := self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		var due schema.WorkList
		if err := conn.List(ctx, &due, schema.QueueDue{Volume: volume, Kinds: kinds, Now: now, Limit: max}); err != nil {
			return err
		}

		// A rescan is claimed on its own, and otherwise the other work which
		// is due, up to the first rescan
		var claim schema.WorkList
		for _, work := range due {
			if work.Kind == store.KindRescan {
				if len(claim) == 0 {
					claim = append(claim, work)
				}
				break
			}
			claim = append(claim, work)
		}
		leased := schema.QueueLease(now.Add(lease))
		for _, work := range claim {
			if err := conn.Update(ctx, &result, schema.QueueKey{Volume: volume, Work: work}, leased); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// Done removes the work, which is claimed, unless it's pushed again since,
// when it's kept, so it's claimed again
func (self *Store) Done(ctx context.Context, volume string, work ...store.Work) (err error) {
	ctx, end := self.span(ctx, "Done", attribute.String("arg.volume", volume), attribute.Int("arg.work", len(work)))
	defer func() { end(err) }()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		for _, w := range work {
			if err := conn.Delete(ctx, nil, schema.QueueKey{Volume: volume, Work: w}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// Failed counts an attempt of the work, which is claimed, with why it
// failed, and makes it due again after the retry. Work which isn't claimed,
// as it's pushed again since, isn't counted.
func (self *Store) Failed(ctx context.Context, volume string, work store.Work, cause error, retry time.Duration) (err error) {
	ctx, end := self.span(ctx, "Failed", attribute.String("arg.volume", volume), attribute.String("arg.key", work.Key))
	defer func() { end(err) }()
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	var result schema.WorkList
	if err := self.conn.Update(ctx, &result, schema.QueueKey{Volume: volume, Work: work}, schema.QueueFailure{Error: message, After: time.Now().Add(retry)}); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
		return storeErr(err)
	}
	return nil
}

// Pending returns true when the key has work queued, or claimed, which
// would change its index: an index of it, or a rescan of it, or of a folder
// above it
func (self *Store) Pending(ctx context.Context, volume, key string) (_ bool, err error) {
	ctx, end := self.span(ctx, "Pending", attribute.String("arg.volume", volume), attribute.String("arg.key", key))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return false, err
	}
	var pending schema.Pending
	if err := self.conn.Get(ctx, &pending, schema.QueuePending{Volume: volume, Key: key}); err != nil {
		return false, storeErr(err)
	}
	return bool(pending), nil
}

// PendingBeneath returns true when the folder's key has work queued, or
// claimed, which would change its listing: an index of it, or a rescan of
// it, or of a folder above it, or an index, or a rescan, of a key beneath it
func (self *Store) PendingBeneath(ctx context.Context, volume, folder string) (_ bool, err error) {
	ctx, end := self.span(ctx, "PendingBeneath", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder))
	defer func() { end(err) }()
	if err := store.ValidateWork(store.Work{Key: folder, Kind: store.KindRescan}); err != nil {
		return false, err
	}
	var pending schema.Pending
	if err := self.conn.Get(ctx, &pending, schema.QueuePending{Volume: volume, Key: folder, Beneath: true}); err != nil {
		return false, storeErr(err)
	}
	return bool(pending), nil
}
