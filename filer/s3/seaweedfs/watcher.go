package seaweedfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"path"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer_pb "github.com/mutablelogic/go-filer/filer/s3/seaweedfs/filer_pb"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	grpc "google.golang.org/grpc"
	insecure "google.golang.org/grpc/credentials/insecure"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// seen are the changes of the latest time which are sent, as a filer can send
// a change more than once, and when it's subscribed to again, the change of
// the time it's subscribed from
type seen struct {
	ts      int64
	changes map[string]bool
}

// Watcher watches the changes to the objects of a bucket of a filer, beneath
// a prefix, by subscribing to the changes to the filer's metadata
type Watcher struct {
	conn      *grpc.ClientConn
	client    filer_pb.SeaweedFilerClient
	bucketDir string             // the filer's directory of the bucket, which ends with "/"
	dir       string             // the filer's directory of the objects, which ends with "/"
	closed    context.Context    // cancelled when the watcher is closed, which ends watching
	close     context.CancelFunc // cancels closed
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// clientName is the name of the subscriber, which the filer logs
	clientName = "go-filer"

	// uploadsDir is the directory of a bucket where SeaweedFS keeps the
	// parts of multipart uploads, which aren't objects
	uploadsDir = ".uploads"

	// versionsSuffix is the suffix of the directory of a key's versions, in
	// a versioned bucket, whose entries are the versions, and not objects
	versionsSuffix = ".versions"

	// configTimeout is how long the filer's configuration is waited for
	configTimeout = 10 * time.Second

	// maxFailures is how many times subscribing can fail in a row, before
	// it's an error, rather than subscribing again
	maxFailures = 5
)

var (
	// retryMin and retryMax are how long subscribing again is waited for,
	// which doubles each time it fails
	retryMin = time.Second
	retryMax = 30 * time.Second
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns a watcher of the objects of the bucket, beneath the prefix,
// of the filer whose gRPC service is at the address, as host:port, such as
// 192.168.86.21:18888, which is the filer's port, plus 10000. The filer's
// configuration is got, which is its directory of buckets, so a filer which
// can't be reached is an error.
func New(ctx context.Context, addr, bucket, prefix string) (*Watcher, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" && !isPath(prefix) {
		return nil, gofiler.ErrBadParameter.Withf("Invalid prefix %q", prefix)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("SeaweedFS filer %q: %v", addr, err)
	}
	self := &Watcher{conn: conn, client: filer_pb.NewSeaweedFilerClient(conn)}

	// The filer's directory of buckets
	ctx, cancel := context.WithTimeout(ctx, configTimeout)
	defer cancel()
	config, err := self.client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
	if err != nil {
		return nil, errors.Join(gofiler.ErrServiceUnavailable.Withf("SeaweedFS filer %q: %v", addr, err), conn.Close())
	} else if config.DirBuckets == "" {
		return nil, errors.Join(gofiler.ErrServiceUnavailable.Withf("SeaweedFS filer %q has no directory of buckets", addr), conn.Close())
	}
	self.bucketDir = strings.TrimSuffix(config.DirBuckets, "/") + "/" + bucket + "/"
	self.dir = self.bucketDir
	if prefix != "" {
		self.dir += prefix + "/"
	}
	self.closed, self.close = context.WithCancel(context.Background())

	// Return success
	return self, nil
}

// Close the connection to the filer, which ends watching
func (self *Watcher) Close() error {
	self.close()
	return self.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Watch sends the changes to the objects to the channel, from now, until the
// context is cancelled, when it returns nil, or subscribing fails too many
// times in a row. The ready function, when it's set, is called once, when the
// changes are first subscribed to, which are from when it's called, so none
// are missed. When subscribing fails, it's subscribed to again, from the
// last change, after a while, which doubles while it fails.
func (self *Watcher) Watch(ctx context.Context, ch chan<- schema.Event, ready func()) error {
	// Watching ends when the watcher is closed, too
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(self.closed, cancel)()

	// The subscriber is the same, as the filer tells them apart, while it's
	// watched, with an epoch which is higher each time it's subscribed, which
	// the filer needs, to replace the subscription before
	since, retry, failures := time.Now().UnixNano(), retryMin, 0
	id, epoch := rand.Int32(), int32(0)
	seen := &seen{}
	subscribed := func() {
		if ready != nil {
			ready()
			ready = nil
		}
	}
	for {
		epoch++
		received, err := self.subscribe(ctx, ch, id, epoch, since, seen, subscribed)
		if seen.ts > since {
			since = seen.ts
		}
		if ctx.Err() != nil {
			return nil
		} else if received {
			failures, retry = 0, retryMin
		}
		if failures++; failures >= maxFailures {
			return gofiler.ErrServiceUnavailable.Withf("SeaweedFS filer: %v", err)
		}

		// Wait to subscribe again
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
		retry = min(retry*2, retryMax)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// subscribe subscribes to the changes since the time, as the subscriber of
// the epoch, and sends them to the channel, until it fails, or the context
// is cancelled, and returns whether any were received which weren't seen,
// which it skips, and calls subscribed once the subscription is made
func (self *Watcher) subscribe(ctx context.Context, ch chan<- schema.Event, id, epoch int32, since int64, seen *seen, subscribed func()) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := self.client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
		ClientName:  clientName,
		PathPrefix:  self.dir,
		SinceNs:     since,
		ClientId:    id,
		ClientEpoch: epoch,
	})
	if err != nil {
		return false, err
	}
	subscribed()

	// Send the changes
	received := false
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return received, nil
		} else if err != nil {
			return received, err
		}
		if !seen.add(response) {
			continue
		}
		received = true
		for _, event := range self.events(response) {
			select {
			case ch <- event:
			case <-ctx.Done():
				return received, ctx.Err()
			}
		}
	}
}

// events returns the changes to the objects of a change to the filer's
// metadata: an entry which is created, updated or deleted, or which is
// renamed, which is deleted from its path, and created at the other, but
// not those which aren't objects of the bucket, beneath the prefix
func (self *Watcher) events(response *filer_pb.SubscribeMetadataResponse) []schema.Event {
	notification := response.EventNotification
	if notification == nil {
		return nil
	}
	old, new := notification.OldEntry, notification.NewEntry
	oldPath, newPath := "", ""
	if old != nil {
		oldPath = self.path(response.Directory, old.Name)
	}
	if new != nil {
		dir := response.Directory
		if notification.NewParentPath != "" {
			dir = notification.NewParentPath
		}
		newPath = self.path(dir, new.Name)
	}

	// A change to a version of a key, in a versioned bucket, is a change to
	// the directory of the key, which is rescanned, as the version which is
	// the key's isn't known
	oldDir, oldVersion := versionedDir(oldPath)
	newDir, newVersion := versionedDir(newPath)
	if oldVersion || newVersion {
		var events []schema.Event
		if oldVersion {
			events = append(events, event(schema.EventTypeRescan, oldDir))
		}
		if newVersion && (!oldVersion || newDir != oldDir) {
			events = append(events, event(schema.EventTypeRescan, newDir))
		}
		return events
	}

	// The changes, of the paths which are objects
	var events []schema.Event
	switch {
	case oldPath != "" && newPath != "" && oldPath == newPath:
		events = append(events, event(schema.EventTypeUpdated, newPath))
	default:
		if oldPath != "" {
			events = append(events, event(schema.EventTypeDeleted, oldPath))
		}
		if newPath != "" {
			events = append(events, event(schema.EventTypeCreated, newPath))
		}
	}
	return events
}

// add returns true when the change is new, and records it, which is false
// when it's of a time before the latest, or is the same change, of the same
// time, as one which is seen
func (s *seen) add(response *filer_pb.SubscribeMetadataResponse) bool {
	if response.TsNs < s.ts {
		return false
	} else if response.TsNs > s.ts {
		s.ts, s.changes = response.TsNs, map[string]bool{}
	}
	notification := response.EventNotification
	key := fmt.Sprintf("%s\x00%s\x00%s\x00%s", response.Directory, entryName(notification.GetOldEntry()), entryName(notification.GetNewEntry()), notification.GetNewParentPath())
	if s.changes[key] {
		return false
	}
	s.changes[key] = true
	return true
}

// entryName returns the name of the entry, or empty when there's none
func entryName(entry *filer_pb.Entry) string {
	if entry == nil {
		return ""
	}
	return entry.Name
}

// path returns the path of the object of the entry, with the name, in the
// filer's directory, relative to the objects' directory, or empty, when it
// isn't beneath it, or it's an upload's, or isn't the path of an object
func (self *Watcher) path(dir, name string) string {
	full := strings.TrimSuffix(dir, "/") + "/" + name
	if key, ok := strings.CutPrefix(full, self.bucketDir); !ok || key == uploadsDir || strings.HasPrefix(key, uploadsDir+"/") {
		return ""
	} else if rel, ok := strings.CutPrefix(full, self.dir); !ok || rel == "" || !isPath(rel) {
		return ""
	} else {
		return "/" + rel
	}
}

// versionedDir returns the directory of a key, whose version is the path, and
// true, when it's beneath the directory of the key's versions, such as
// /a/b.jpg.versions/v1, which is /a
func versionedDir(p string) (string, bool) {
	elems := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, elem := range elems {
		if len(elem) > len(versionsSuffix) && strings.HasSuffix(elem, versionsSuffix) && i < len(elems)-1 {
			return "/" + path.Join(elems[:i]...), true
		}
	}
	return "", false
}

// isPath returns true when the path has no elements which are empty, or "."
// or "..", so it's a path of an object, beneath its directory
func isPath(p string) bool {
	for _, elem := range strings.Split(p, "/") {
		if elem == "" || elem == "." || elem == ".." {
			return false
		}
	}
	return true
}

// event returns the change of the path
func event(eventType schema.EventType, path string) schema.Event {
	return schema.Event{EventType: eventType, ObjectKey: schema.ObjectKey{Path: path}}
}
