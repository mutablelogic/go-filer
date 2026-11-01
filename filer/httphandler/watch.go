package httphandler

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"slices"
	"sync"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// watchQuery is the query for watching the changes to objects
type watchQuery struct {
	Name []string `json:"name,omitempty" help:"Volumes to watch, which is every volume when there are none"`
}

// deadlineWriter is a writer of a stream to a client, with a deadline for
// each write, which closes failed when a write fails, as the client stopped
// reading, or went away. Once it's stopping, the deadline is soon, which
// interrupts a write which is blocked, and isn't extended.
type deadlineWriter struct {
	http.ResponseWriter
	rc     *http.ResponseController
	once   sync.Once
	failed chan struct{}

	sync.Mutex
	until time.Time // the deadline of every write, once it's stopping
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// watchBuffer is how many changes can wait to be streamed to a client
	watchBuffer = 1024

	// watchWriteTimeout is how long a write to a client can take
	watchWriteTimeout = 30 * time.Second

	// watchStopTimeout is how long the writes to a client can take, once the
	// stream is ending, including the shutdown event
	watchStopTimeout = time.Second

	// EventShutdown is the event which is sent when the server is stopping,
	// before the stream ends
	EventShutdown = "shutdown"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func watchHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	router.Spec().AddTag("Watch", documentation.Section(2, "Watch Operations").Body)
	return router.Register("watch", nil, func(path httprequest.PathItem) {
		path.Tag("Watch")

		// GET
		path.Get(watchHandler(registry), func(op httprequest.PathOperation) {
			op.Summary("Watch Objects")
			op.Description(documentation.Section(3, "GET /watch").Body)
			op.Query(jsonschema.MustFor[watchQuery]())
			op.TextStreamResponse(http.StatusOK)
			op.ErrorResponse(http.StatusBadRequest, "Invalid query")
			op.ErrorResponse(http.StatusNotAcceptable, "The response can't be any of the accepted types")
			op.ErrorResponse(http.StatusServiceUnavailable, "The server is stopping")
		})
	})
}

// watchHandler streams the changes to the objects of the volumes, as
// server-sent events, whose names are the changes, and whose data are the
// objects' keys, until the client goes away, or the server stops, when a
// shutdown event is sent before the stream ends
func watchHandler(registry *filer.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var query watchQuery
		if err := httprequest.Query(r.URL.Query(), &query); err != nil {
			httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			return
		}

		// Changes are only streamed
		if accept, err := types.AcceptContentType(r); err != nil {
			httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			return
		} else if accept != "" && accept != types.ContentTypeAny && accept != types.ContentTypeTextStream {
			httpresponse.Error(w, httpresponse.Err(http.StatusNotAcceptable).Withf("cannot respond with %q", accept))
			return
		}

		// The stream lasts as long as the client watches, so it isn't cut off
		// by the server's write timeout, but each write has its own deadline.
		dw := &deadlineWriter{ResponseWriter: w, rc: http.NewResponseController(w), failed: make(chan struct{})}
		if err := dw.rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			httpresponse.Error(w, httpresponse.ErrInternalError.With(err.Error()))
			return
		}

		// Watch the volumes, until the client goes away, or the registry is
		// closed, as the server is stopping, when a write to a client which
		// stopped reading is interrupted, as the stream is written from
		// another goroutine, which isn't waiting for the registry. The watch
		// ends before the handler returns, as the writer can't be used after.
		ctx, cancel := context.WithCancel(r.Context())
		ch, done, watched := make(chan schema.Event, watchBuffer), make(chan error, 1), make(chan struct{})
		defer func() {
			cancel()
			<-watched
		}()
		go func() {
			defer close(watched)
			err := registry.WatchObjects(ctx, ch)
			if errors.Is(err, fs.ErrClosed) {
				dw.stop()
			}
			done <- err
		}()

		// A proxy, such as nginx, doesn't buffer the stream, which ends soon
		// once the handler returns, even when the client stopped reading
		stream := httpresponse.NewTextStream(dw, "X-Accel-Buffering", "no")
		if stream == nil {
			httpresponse.Error(w, httpresponse.ErrInternalError.With("streaming isn't supported"))
			return
		}
		defer func() {
			dw.stop()
			stream.Close()
		}()

		// Stream the changes, of the volumes which are watched
		for {
			select {
			case <-ctx.Done():
				return
			case <-dw.failed:
				return
			case err := <-done:
				// The registry is closed, as the server is stopping, so the
				// client is told, and the stream ends. The event has data, as
				// a browser doesn't dispatch one without any.
				if errors.Is(err, fs.ErrClosed) {
					stream.Write(EventShutdown, struct{}{})
				}
				return
			case event := <-ch:
				if len(query.Name) == 0 || slices.Contains(query.Name, event.Name) {
					stream.Write(event.EventType.String(), event.ObjectKey)
				}
			}
		}
	}
}

// Write writes to the client, by the deadline
func (w *deadlineWriter) Write(data []byte) (int, error) {
	w.deadline()
	n, err := w.ResponseWriter.Write(data)
	if err != nil {
		w.fail()
	}
	return n, err
}

// Flush sends what's written to the client, by the deadline
func (w *deadlineWriter) Flush() {
	w.deadline()
	if err := w.rc.Flush(); err != nil {
		w.fail()
	}
}

// Unwrap returns the response writer, for a response controller
func (w *deadlineWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// deadline sets the deadline of the next write, which is no later than when
// it's stopping
func (w *deadlineWriter) deadline() {
	w.Lock()
	defer w.Unlock()
	deadline := time.Now().Add(watchWriteTimeout)
	if !w.until.IsZero() && w.until.Before(deadline) {
		deadline = w.until
	}
	w.setDeadline(deadline)
}

// stop sets the deadline of the writes, including one which is blocked, to
// soon, once, as the stream is ending
func (w *deadlineWriter) stop() {
	w.Lock()
	defer w.Unlock()
	if w.until.IsZero() {
		w.until = time.Now().Add(watchStopTimeout)
		w.setDeadline(w.until)
	}
}

// setDeadline sets the deadline of the writes, which must be locked
func (w *deadlineWriter) setDeadline(deadline time.Time) {
	if err := w.rc.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.fail()
	}
}

// fail closes the channel which says a write failed
func (w *deadlineWriter) fail() {
	w.once.Do(func() { close(w.failed) })
}
