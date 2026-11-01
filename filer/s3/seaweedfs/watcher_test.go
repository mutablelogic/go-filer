package seaweedfs

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer_pb "github.com/mutablelogic/go-filer/filer/s3/seaweedfs/filer_pb"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// fakeFiler is a filer, whose directory of buckets is /buckets, which sends
// the responses of its channel to each subscriber, and records their
// requests, and ends a subscription with an error, when it's sent nil
type fakeFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	sync.Mutex
	responses chan *filer_pb.SubscribeMetadataResponse
	requests  []*filer_pb.SubscribeMetadataRequest
}

func (f *fakeFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets"}, nil
}

func (f *fakeFiler) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	f.Lock()
	f.requests = append(f.requests, req)
	f.Unlock()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case response := <-f.responses:
			if response == nil {
				return status.Error(codes.Unavailable, "unavailable")
			} else if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

// subscriptions returns the requests to subscribe
func (f *fakeFiler) subscriptions() []*filer_pb.SubscribeMetadataRequest {
	f.Lock()
	defer f.Unlock()
	return append([]*filer_pb.SubscribeMetadataRequest(nil), f.requests...)
}

// newFakeFiler returns a filer, and its address
func newFakeFiler(t *testing.T) (*fakeFiler, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	filer := &fakeFiler{responses: make(chan *filer_pb.SubscribeMetadataResponse)}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return filer, listener.Addr().String()
}

// change returns a change to the filer's metadata, in the directory
func change(ts int64, dir string, old, new *filer_pb.Entry, newParent string) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		TsNs:      ts,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: old, NewEntry: new, NewParentPath: newParent,
		},
	}
}

// entry returns an entry, with the name
func entry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name}
}

// receive returns the next change, or fails the test
func receive(t *testing.T, ch chan schema.Event) schema.Event {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("no change")
		return schema.Event{}
	}
}

func Test_Watcher(t *testing.T) {
	filer, addr := newFakeFiler(t)
	watcher, err := New(context.Background(), addr, "media", "photos")
	require.NoError(t, err)
	defer watcher.Close()

	// Watch the changes, which are subscribed to from now, beneath the
	// prefix, once it's ready
	ctx, cancel := context.WithCancel(context.Background())
	ch, ready, done := make(chan schema.Event, 10), make(chan struct{}), make(chan error, 1)
	start := time.Now().UnixNano()
	base := start + int64(time.Hour) // the times of the changes, which are after watching starts
	go func() {
		done <- watcher.Watch(ctx, ch, func() { close(ready) })
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("not ready")
	}

	// Entries which are created, updated, deleted and renamed, within the
	// prefix, or into it, or out of it, are changes to their objects
	dir := "/buckets/media/photos"
	for _, response := range []*filer_pb.SubscribeMetadataResponse{
		change(base+1, dir, nil, entry("a.jpg"), ""),
		change(base+2, dir+"/2026", entry("b.jpg"), entry("b.jpg"), ""),
		change(base+3, dir, entry("a.jpg"), nil, ""),
		change(base+4, dir, entry("c.jpg"), entry("d.jpg"), dir),
		change(base+5, dir, entry("e.jpg"), entry("e.jpg"), "/buckets/media/other"),
		change(base+6, "/buckets/media/other", entry("f.jpg"), entry("f.jpg"), dir+"/2026"),
		change(base+7, dir, nil, &filer_pb.Entry{Name: "dir", IsDirectory: true}, ""),
	} {
		filer.responses <- response
	}
	for _, want := range []schema.Event{
		{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/a.jpg"}},
		{EventType: schema.EventTypeUpdated, ObjectKey: schema.ObjectKey{Path: "/2026/b.jpg"}},
		{EventType: schema.EventTypeDeleted, ObjectKey: schema.ObjectKey{Path: "/a.jpg"}},
		{EventType: schema.EventTypeDeleted, ObjectKey: schema.ObjectKey{Path: "/c.jpg"}},
		{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/d.jpg"}},
		{EventType: schema.EventTypeDeleted, ObjectKey: schema.ObjectKey{Path: "/e.jpg"}},
		{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/2026/f.jpg"}},
		{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/dir"}},
	} {
		assert.Equal(t, want, receive(t, ch))
	}

	// Entries which aren't objects of the prefix, uploads, or aren't paths,
	// aren't changes
	for _, response := range []*filer_pb.SubscribeMetadataResponse{
		change(base+8, "/buckets/media/photos2", nil, entry("x.jpg"), ""),
		change(base+9, "/buckets/media", nil, entry("photos"), ""),
		change(base+10, dir+"/.uploads/123", nil, entry("1.part"), ""),
		change(base+11, dir, nil, entry(".."), ""),
		{TsNs: base + 12},
		change(base+13, dir, nil, entry("last.jpg"), ""),
	} {
		filer.responses <- response
	}
	assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/last.jpg"}}, receive(t, ch))

	// A change which is sent again, of the same time, as a filer can, or one
	// of a time before the latest, is sent once
	filer.responses <- change(base+13, dir, nil, entry("last.jpg"), "")
	filer.responses <- change(base+13, dir, nil, entry("other.jpg"), "")
	filer.responses <- change(base+13, dir, nil, entry("other.jpg"), "")
	filer.responses <- change(base+1, dir, nil, entry("old.jpg"), "")
	filer.responses <- change(base+13, dir, nil, entry("next.jpg"), "")
	assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/other.jpg"}}, receive(t, ch))
	assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/next.jpg"}}, receive(t, ch))

	// When the subscription fails, it's subscribed to again, from the last
	// change, and the requests are of the prefix
	defer func(retry time.Duration) { retryMin = retry }(retryMin)
	retryMin = 10 * time.Millisecond
	filer.responses <- nil
	filer.responses <- change(base+14, dir, nil, entry("again.jpg"), "")
	assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/again.jpg"}}, receive(t, ch))
	requests := filer.subscriptions()
	require.Len(t, requests, 2)
	assert.Equal(t, dir+"/", requests[0].PathPrefix)
	assert.GreaterOrEqual(t, requests[0].SinceNs, start)
	assert.Equal(t, base+13, requests[1].SinceNs)
	assert.Equal(t, requests[0].ClientId, requests[1].ClientId)

	// Watching ends when it's cancelled
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("watching didn't end")
	}
}

func Test_WatcherFails(t *testing.T) {
	defer func(retry time.Duration) { retryMin = retry }(retryMin)
	retryMin = time.Millisecond
	filer, addr := newFakeFiler(t)
	watcher, err := New(context.Background(), addr, "media", "")
	require.NoError(t, err)
	defer watcher.Close()

	// Subscribing which fails too many times in a row is an error
	done := make(chan error, 1)
	go func() {
		done <- watcher.Watch(context.Background(), make(chan schema.Event), nil)
	}()
	for range maxFailures {
		filer.responses <- nil
	}
	select {
	case err := <-done:
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("watching didn't fail")
	}
	assert.Equal(t, "/buckets/media/", filer.subscriptions()[0].PathPrefix)
}

func Test_WatcherNotFound(t *testing.T) {
	// A filer which can't be reached is an error
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()
	_, err = New(context.Background(), addr, "media", "")
	assert.True(t, errors.Is(err, gofiler.ErrServiceUnavailable), err)
}
