package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadataMime "github.com/mutablelogic/go-filer/metadata/mime"
	pg "github.com/mutablelogic/go-pg"
	pgqueueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (filer *Filer) GetObject(ctx context.Context, req schema.ObjectKey) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "GetObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := filer.GetVolume(ctx, req.Volume)
	if err != nil {
		return nil, err
	} else if types.Value(volume.Enabled) == false {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Get the backend
	backend := filer.volumes.Get(volume.Name)
	if backend == nil {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Get the object from the backend first
	object, err := backend.GetObject(ctx, schema.GetObjectRequest{
		ObjectKey: req,
	})
	if err != nil {
		return nil, err
	}

	// If the volume is not indexed, or the index delta is zero, return the backend object directly
	if types.Value(volume.IndexDelta) == 0 || volume.IndexedAt == nil {
		return object, nil
	}

	// Get the metadata from the database (on error, return the object we have)
	var result schema.Object
	if err := filer.PoolConn.Get(ctx, &result, req); errors.Is(err, pg.ErrNotFound) {
		// Kick off an indexing job for the object
		return object, filer.enqueueIndexObject(ctx, object.ObjectKey, true)
	} else if err != nil {
		return object, err
	}

	// If the objects match, return the result from the database
	if result.Matches(object) {
		return types.Ptr(result), nil
	}

	// Kick off an indexing job for the object, and return the object we have
	if err := filer.enqueueIndexObject(ctx, object.ObjectKey, true); err != nil {
		return object, err
	}

	// Return the backend object in preference to the database object, since the database object is stale
	return object, nil
}

func (filer *Filer) ListObjects(ctx context.Context, req schema.ObjectListRequest) (_ *schema.ObjectList, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "ListObjects",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := filer.GetVolume(ctx, req.Volume)
	if err != nil {
		return nil, err
	} else if types.Value(volume.Enabled) == false {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Check prefix/path argument
	if path := types.Value(req.Path); path != "" {
		// TODO: Path should be a valid prefix or valid object
		// or else return NotFound
	}

	// If indexing is not enabled, or no indexing has happened yet, or there is no last indexed object,
	// iterate the backend directly.
	if types.Value(volume.IndexDelta) == 0 || volume.IndexedAt == nil || volume.LastIndexedObjectAt == nil {
		b := filer.volumes.Get(volume.Name)
		if b == nil {
			return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
		}

		offset := req.Offset
		limit := uint64(schema.ObjectListLimit)
		if req.Limit != nil {
			limit = *req.Limit
		}

		iterator := &schema.ObjectListIterator{
			Path:      req.Path,
			Type:      req.Type,
			Recursive: req.Recursive,
		}

		var result schema.ObjectList
		n := uint64(0)
	outer:
		for {
			err := b.ListObjects(ctx, iterator)
			done := errors.Is(err, io.EOF)
			if err != nil && !done {
				return nil, err
			}
			for _, obj := range iterator.Body {
				// The backend only distinguishes directories from files via Type;
				// apply the actual content-type filter here.
				if !schema.MatchesContentType(obj.ContentType, types.Value(req.Type)) {
					continue
				}
				if n >= offset {
					if uint64(len(result.Body)) >= limit {
						break outer
					}
					result.Body = append(result.Body, obj)
				}
				n++
			}
			if done {
				break
			}
		}

		req.Path = iterator.Path // reflect normalised path back into the response
		result.ObjectListRequest = req
		result.OffsetLimit.Limit = types.Ptr(limit)
		return types.Ptr(result), nil
	}

	// Use the database index to return the objects
	if types.Value(req.Type) == schema.ContentTypeDirectory {
		return filer.listIndexedDirectories(ctx, req)
	}

	var result schema.ObjectList
	if err := filer.PoolConn.List(ctx, &result, &req); err != nil {
		return nil, err
	} else {
		result.ObjectListRequest = req
		result.OffsetLimit.Clamp(uint64(result.Count))
	}

	// Return success
	return types.Ptr(result), nil
}

func (filer *Filer) listIndexedDirectories(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	objectReq := req
	objectReq.Type = nil
	objectReq.Recursive = true
	objectReq.Offset = 0
	objectReq.Limit = types.Ptr(uint64(schema.ObjectListLimit))

	var objects schema.ObjectList
	if err := filer.PoolConn.List(ctx, &objects, &objectReq); err != nil {
		return nil, err
	}

	reqPath := strings.TrimPrefix(strings.TrimSuffix(types.NormalisePath(types.Value(req.Path)), "/"), "/")
	seen := make(map[string]bool)
	dirs := make([]*schema.Object, 0)
	for _, obj := range objects.Body {
		parts := strings.Split(obj.Path, "/")
		for i := 1; i < len(parts); i++ {
			dir := strings.Join(parts[:i], "/")
			if dir == reqPath || seen[dir] {
				continue
			}
			if reqPath != "" {
				if !strings.HasPrefix(dir, reqPath+"/") {
					continue
				}
				remainder := strings.TrimPrefix(dir, reqPath+"/")
				if !req.Recursive && strings.Contains(remainder, "/") {
					continue
				}
			} else if !req.Recursive && strings.Contains(dir, "/") {
				continue
			}
			seen[dir] = true
			dirs = append(dirs, &schema.Object{
				ObjectKey: schema.ObjectKey{Volume: req.Volume, Path: dir},
				ObjectMeta: schema.ObjectMeta{
					ContentType: schema.ContentTypeDirectory,
				},
			})
		}
	}

	result := schema.ObjectList{
		ObjectListRequest: req,
		Count:             len(dirs),
		Body:              dirs,
	}
	result.OffsetLimit.Clamp(uint64(result.Count))
	return types.Ptr(result), nil
}

func (filer *Filer) CreateObject(ctx context.Context, req schema.CreateObjectRequest, attr schema.ObjectAttr) (_ *schema.Object, _ bool, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "CreateObject",
		attribute.String("req", types.Stringify(req)),
		attribute.String("attr", types.Stringify(attr)),
	)
	defer func() {
		if errors.Is(err, gofiler.ErrConflict) || errors.Is(err, gofiler.ErrNotModified) {
			endSpan(nil)
		} else {
			endSpan(err)
		}
	}()

	// Check the volume first
	volume, err := filer.GetVolume(ctx, req.Volume)
	if err != nil {
		return nil, false, err
	} else if types.Value(volume.Enabled) == false {
		return nil, false, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Get the backend
	backend := filer.volumes.Get(volume.Name)
	if backend == nil {
		return nil, false, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	created := false
	current, err := backend.GetObject(ctx, schema.GetObjectRequest{
		ObjectKey: req.ObjectKey,
	})
	if errors.Is(err, gofiler.ErrNotFound) {
		created = true
	} else if err != nil {
		return nil, false, err
	} else if req.IfNotExists {
		return nil, false, gofiler.ErrConflict.Withf("object already exists: %q", req.Path)
	} else if createObjectHasPreconditions(req, attr) && createObjectNotModified(current, attr) {
		return current, false, gofiler.ErrNotModified
	}

	// Create the object in the backend. The backend owns IfNotExists semantics.
	object, err := backend.CreateObject(ctx, req)
	if err != nil {
		return nil, false, err
	}

	// Kick off an indexing job for the object, and return the backend object.
	return object, created, filer.enqueueIndexObject(ctx, object.ObjectKey, true)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (filer *Filer) touchObject(ctx context.Context, req schema.ObjectKey) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "touchObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.Object
	if err := filer.Tx(ctx, func(conn pg.Conn) error {
		return conn.Update(ctx, &result, schema.ObjectTouch(req), nil)
	}); err != nil {
		return nil, err
	}
	return types.Ptr(result), nil
}

func createObjectHasPreconditions(req schema.CreateObjectRequest, attr schema.ObjectAttr) bool {
	return req.IfNotExists || attr.ETag != nil || !attr.ModTime.IsZero()
}

func createObjectNotModified(current *schema.Object, attr schema.ObjectAttr) bool {
	if current == nil {
		return false
	}
	if attr.Size != nil && (current.Size == nil || types.Value(current.Size) != types.Value(attr.Size)) {
		return false
	}
	if attr.ETag != nil {
		return current.ETag != nil && types.Value(current.ETag) == types.Value(attr.ETag)
	}
	return !attr.ModTime.IsZero() && !current.ModTime.IsZero() && current.ModTime.Truncate(time.Second).Equal(attr.ModTime.Truncate(time.Second))
}

func (filer *Filer) enqueueIndexObject(ctx context.Context, key schema.ObjectKey, force bool) error {
	if enabled, err := filer.indexingEnabled(ctx, key.Volume); err != nil {
		return err
	} else if !enabled {
		return nil
	}

	payload, err := json.Marshal(indexObjectTask{ObjectKey: key, Force: force})
	if err != nil {
		return fmt.Errorf("failed to marshal index task: %w", err)
	}
	_, err = filer.queue.CreateTask(ctx, filer.indexQueue.Queue, pgqueueschema.TaskMeta{
		Payload: payload,
	})
	return err
}

func (filer *Filer) indexingEnabled(ctx context.Context, volume string) (bool, error) {
	v, err := filer.GetVolume(ctx, volume)
	if err != nil {
		return false, err
	}
	return types.Value(v.IndexDelta) != 0, nil
}

func (filer *Filer) createObject(ctx context.Context, req schema.ObjectCreate, artwork []*schema.ArtworkMeta) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "createObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// If the content type is not provided, default to application/octet-stream
	if typ := strings.TrimSpace(req.ContentType); typ == "" {
		req.ContentType = types.ContentTypeBinary
	} else if t, params, err := mime.ParseMediaType(typ); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid content type: %q", typ)
	} else if t == schema.ContentTypeDirectory {
		return nil, gofiler.ErrBadParameter.Withf("invalid content type: %q", typ)
	} else {
		req.ContentType = t
		for key, v := range params {
			if key = strings.TrimSpace(key); types.IsIdentifier(key) {
				req.Meta = schema.AppendMeta(req.Meta, key, v)
			}
		}
	}

	// Upsert the object
	var result schema.Object
	if err := filer.Tx(ctx, func(conn pg.Conn) error {
		// Delete the object first, to clear the metadata
		if err := conn.Delete(ctx, &result, req.ObjectKey); errors.Is(err, pg.ErrNotFound) {
			// Ignore not found errors, we will insert a new object
		} else if err != nil {
			return err
		}

		// Insert the object
		if err := conn.Insert(ctx, &result, req); err != nil {
			return err
		}

		// Insert the metadata
		for _, meta := range req.ObjectMeta.Meta {
			var metaresult schema.Meta
			if err := conn.With("volume", result.Volume, "path", result.Path).Insert(ctx, &metaresult, meta); err != nil {
				return err
			} else {
				result.Meta = append(result.Meta, metaresult)
			}
		}

		// Insert the artwork
		for _, meta := range artwork {
			var artwork schema.Artwork
			if err := conn.Insert(ctx, &artwork, meta); err != nil {
				return err
			}
			var link schema.ObjectArtwork
			if err := conn.Insert(ctx, &link, schema.ObjectArtwork{
				ObjectKey:  result.ObjectKey,
				ArtworkKey: artwork.ETag,
			}); err != nil {
				return err
			}
		}

		// Return success
		return nil
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Re-fetch the object to get the metadata and artwork
	if err := filer.PoolConn.Get(ctx, &result, req.ObjectKey); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return the inserted object
	return types.Ptr(result), nil
}

func (filer *Filer) indexObject(ctx context.Context, key schema.ObjectKey, force bool) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "indexObject",
		attribute.String("object", types.Stringify(key)),
		attribute.Bool("force", force),
	)
	defer func() {
		if errors.Is(err, gofiler.ErrNotFound) {
			endSpan(nil)
		} else {
			endSpan(err)
		}
	}()

	if enabled, err := filer.indexingEnabled(ctx, key.Volume); err != nil {
		return nil, err
	} else if !enabled {
		return nil, nil
	}

	// Obtain the backend of the object - backend might be disabled, so don't error
	backend := filer.volumes.Get(key.Volume)
	if backend == nil {
		return nil, nil
	}

	// Read the object from the backend and extract metadata
	reader, object, err := backend.ReadObject(ctx, schema.GetObjectRequest{
		ObjectKey: key,
	})

	if errors.Is(err, gofiler.ErrNotFound) {
		// Object no longer exists in the backend — remove it from the index
		var deleted schema.Object
		if delErr := filer.Tx(ctx, func(conn pg.Conn) error {
			return conn.Delete(ctx, &deleted, key)
		}); delErr != nil && !errors.Is(delErr, pg.ErrNotFound) {
			return nil, delErr
		}
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	// Unless forced, check if the object has changed in index and skip re-indexing if not
	if !force {
		// Get the object from the database
		var existing schema.Object
		if err := filer.PoolConn.Get(ctx, &existing, object.ObjectKey); err != nil {
			if errors.Is(err, pg.ErrNotFound) == false {
				return nil, err
			}
		}

		// Check if the object has changed in index - if not, touch the indexed_at and return the existing object
		if (existing.ETag != nil && object.ETag != nil && types.Value(existing.ETag) == types.Value(object.ETag)) ||
			(!existing.ModTime.IsZero() && !object.ModTime.IsZero() && existing.ModTime.Truncate(time.Second).Equal(object.ModTime.Truncate(time.Second))) {
			// Object unchanged — touch indexed_at and return the refreshed object
			return filer.touchObject(ctx, object.ObjectKey)
		}
	}

	// Extractors need to seek within the object content. Use the reader directly
	// if it's already seekable (e.g. local files), otherwise buffer it into memory.
	var metaReader io.ReadSeeker
	var metaErr error
	if seeker, ok := reader.(io.ReadSeeker); ok {
		metaReader = seeker
	} else {
		data, err := readAllObject(reader, object)
		if err != nil {
			metaErr = err
		} else {
			metaReader = bytes.NewReader(data)
		}
	}
	if metaReader != nil {
		namedReader := namedReadSeeker{ReadSeeker: metaReader, name: object.Path}
		if contentType, _, err := metadataMime.Type(namedReader); err == nil && contentType != types.ContentTypeBinary {
			object.ContentType = contentType
		}
		if _, err := metaReader.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}

	// Get the metadata for the object. Read and extraction failures should not
	// prevent the object itself from being indexed.
	metadata := []schema.Meta{}
	var artwork []*schema.ArtworkMeta
	if metaReader != nil {
		metadata, artwork, metaErr = filer.metadata.Get(ctx, object.ContentType, metaReader)
		if metaErr != nil && metadata == nil {
			metadata = []schema.Meta{}
			metaErr = fmt.Errorf("extract metadata for %q: %w", object.Path, metaErr)
		}
	}

	// Create the object in a transaction, and touch the volume's indexed_at
	result, err := filer.createObject(ctx, schema.ObjectCreate{
		ObjectKey:  object.ObjectKey,
		ObjectAttr: object.ObjectAttr,
		ObjectMeta: schema.ObjectMeta{
			ContentType: object.ContentType,
			Meta:        metadata,
		},
	}, artwork)
	if err != nil {
		return nil, err
	}

	// Return the result and any metadata extraction error (which is non-fatal)
	return result, metaErr
}

func readAllObject(reader io.Reader, object *schema.Object) ([]byte, error) {
	data, err := io.ReadAll(reader)
	if err == nil {
		return data, nil
	}

	if object == nil {
		return nil, fmt.Errorf("read object content: %w (read=%d)", err, len(data))
	}
	if object.Size == nil {
		return nil, fmt.Errorf("read object content %q: %w (read=%d expected=unknown)", object.Path, err, len(data))
	}
	return nil, fmt.Errorf("read object content %q: %w (read=%d expected=%d)", object.Path, err, len(data), types.Value(object.Size))
}

type namedReadSeeker struct {
	io.ReadSeeker
	name string
}

func (r namedReadSeeker) Name() string {
	return r.name
}
