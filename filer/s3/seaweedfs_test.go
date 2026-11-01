package s3

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer_pb "github.com/mutablelogic/go-filer/filer/s3/seaweedfs/filer_pb"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
)

// fakeSeaweedFS is a SeaweedFS filer, whose directory of buckets is /buckets,
// which sends a change to each subscriber
type fakeSeaweedFS struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (fakeSeaweedFS) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets"}, nil
}

func (fakeSeaweedFS) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream grpc.ServerStreamingServer[filer_pb.SubscribeMetadataResponse]) error {
	stream.Send(&filer_pb.SubscribeMetadataResponse{
		Directory:         req.PathPrefix + "dir",
		TsNs:              time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "a.jpg"}},
	})
	<-stream.Context().Done()
	return nil
}

func Test_S3_WatchSeaweedFS(t *testing.T) {
	endpoint := fakeS3(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fakeSeaweedFS{})
	go server.Serve(listener)
	defer server.Stop()

	// A bucket of a SeaweedFS filer is watched, by its gRPC service, which is
	// ready, and sends the changes to its objects
	b, err := New(context.Background(), bucketURL(t, endpoint, "photos", "&seaweedfs-filer-grpc="+url.QueryEscape(listener.Addr().String())), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, listener.Addr().String(), b.URL().Query().Get("seaweedfs-filer-grpc"))
	ctx, cancel := context.WithCancel(context.Background())
	ch, ready, done := make(chan schema.Event, 1), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- b.WatchObjectsReady(ctx, ch, func() { close(ready) })
	}()
	<-ready
	select {
	case event := <-ch:
		assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/dir/a.jpg"}}, event)
	case <-time.After(5 * time.Second):
		t.Fatal("no change")
	}
	cancel()
	assert.NoError(t, <-done)
	assert.NoError(t, b.Close())

	// A filer which can't be reached is an error, and a bucket which isn't a
	// filer's can't be watched
	listener.Close()
	server.Stop()
	_, err = New(context.Background(), bucketURL(t, endpoint, "photos", "&seaweedfs-filer-grpc="+url.QueryEscape(listener.Addr().String())), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	b, err = New(context.Background(), bucketURL(t, endpoint, "photos", ""), gofiler.Opts{})
	require.NoError(t, err)
	assert.ErrorIs(t, b.WatchObjects(context.Background(), make(chan schema.Event)), gofiler.ErrNotImplemented)
}
