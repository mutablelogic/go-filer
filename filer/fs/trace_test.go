package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func Test_Trace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	self, err := NewFS("test", fstest.MapFS{"a/b.txt": {Data: []byte("b")}}, nil, filer.Opts{Tracer: provider.Tracer("test")})
	require.NoError(t, err)

	// Each operation is a span, with the volume and the path, and its error,
	// which is the object's
	iter, err := self.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a"}, Recursive: true})
	require.NoError(t, err)
	require.NoError(t, iter.Close())
	_, err = self.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/missing.txt"}})
	require.ErrorIs(t, err, fs.ErrNotExist)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	for i, test := range []struct {
		name   string
		req    any
		status codes.Code
	}{
		{"fs.ListObjects", schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a"}, Recursive: true}, codes.Unset},
		{"fs.GetObject", schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/missing.txt"}}, codes.Error},
	} {
		assert.Equal(t, test.name, spans[i].Name())
		attrs := map[attribute.Key]attribute.Value{}
		for _, attr := range spans[i].Attributes() {
			attrs[attr.Key] = attr.Value
		}
		assert.Equal(t, "test", attrs["filer.volume"].AsString())
		assert.Equal(t, types.Stringify(test.req), attrs["arg.req"].AsString())
		assert.Equal(t, test.status, spans[i].Status().Code)
	}
	assert.Contains(t, spans[1].Status().Description, "/missing.txt")
}

func Test_TraceListAndRead(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	files := fstest.MapFS{"a/b.txt": {Data: []byte("b")}, "a/c.txt": {Data: []byte("c")}}
	self, err := NewFS("test", files, nil, filer.Opts{Tracer: provider.Tracer("test")})
	require.NoError(t, err)

	// A listing's span lasts until there are no more objects
	iter, err := self.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a"}})
	require.NoError(t, err)
	assert.Empty(t, recorder.Ended())
	for {
		if _, err := iter.Next(context.Background()); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
	}
	require.Len(t, recorder.Ended(), 1)
	assert.Equal(t, "fs.ListObjects", recorder.Ended()[0].Name())
	assert.Equal(t, codes.Unset, recorder.Ended()[0].Status().Code)
	require.NoError(t, iter.Close())
	assert.Len(t, recorder.Ended(), 1)

	// Reading a file's content is a span, which fails when the file has
	// changed since it was returned
	read, _, err := self.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/b.txt"}})
	require.NoError(t, err)
	require.NoError(t, read(context.Background(), func(io.Reader) error { return nil }))
	read, _, err = self.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/c.txt"}})
	require.NoError(t, err)
	files["a/c.txt"] = &fstest.MapFile{Data: []byte("changed")}
	require.ErrorIs(t, read(context.Background(), func(io.Reader) error { return nil }), filer.ErrPreconditionFailed)

	spans := recorder.Ended()
	require.Len(t, spans, 5)
	assert.Equal(t, "fs.ReadObject", spans[1].Name())
	assert.Equal(t, "fs.ReadObject.Read", spans[2].Name())
	assert.Equal(t, codes.Unset, spans[2].Status().Code)
	assert.Equal(t, "fs.ReadObject.Read", spans[4].Name())
	assert.Equal(t, codes.Error, spans[4].Status().Code)
}
