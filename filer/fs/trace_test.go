package fs

import (
	"context"
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
