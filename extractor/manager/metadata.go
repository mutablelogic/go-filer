package manager

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	extractor "github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

const extractMetadataTimeout = 5 * time.Minute

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) IndexFileAtPath(ctx context.Context, key, path string, info os.FileInfo, force bool, warn *error) (_ *schema.Metadata, err error) {
	// Get metadata for the file at the path
	metadata, err := schema.MetadataFromPath(path, info)
	if err != nil {
		return nil, err
	}

	// Perform the indexing in a transaction, so that if there are any errors, the database changes will be rolled back.
	var result schema.Metadata
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		// Retrieve an existing metadata record for the same key and check for it being modified
		if err := conn.Get(ctx, &result, schema.MetadataKey(key)); errors.Is(err, pg.ErrNotFound) {
			// OK, no existing record
		} else if err != nil {
			return err
		} else if metadata.Etag == result.Etag && !force {
			// No changes needed
			if warn != nil {
				*warn = gofiler.ErrNotIndexed.With("file has not changed since last indexing")
			}
			return nil
		}

		// Extract additional metadata from the file.
		// If extraction is slow or fails, continue indexing base metadata.
		if extractor, err := registry.Get(metadata.Type); err == nil {
			if additional, err := extractMetadataWithTimeout(ctx, extractor, path, extractMetadataTimeout); err == nil {
				for _, kv := range additional {
					metadata.Metadata = append(metadata.Metadata, kv)
				}
			} else if warn != nil {
				*warn = err
			}
		}

		// Copy across some well-known metadata keys to the top-level fields
		for _, kv := range metadata.Metadata {
			switch kv.Key {
			case extractor.TextTitle, extractor.PDFTitle, extractor.AudioTitle, extractor.VideoTitle, extractor.ImageTitle:
				if value, err := strconv.Unquote(string(kv.Value)); err == nil && value != "" {
					metadata.Title = value
				}
			case extractor.VideoDescription, extractor.VideoSynopsis:
				if value, err := strconv.Unquote(string(kv.Value)); err == nil && value != "" {
					metadata.Summary = types.Ptr(value)
				}
			case extractor.TextSummary, extractor.ImageSummary:
				if value, err := strconv.Unquote(string(kv.Value)); err == nil && value != "" {
					metadata.Summary = types.Ptr(value)
				}
			case extractor.TextTags, extractor.ImageTags:
				var tags []string
				if err := json.Unmarshal(kv.Value, &tags); err == nil && len(tags) > 0 {
					metadata.Tags = tags
				}
			}
		}

		// Insert or replace the metadata record for the key
		if err := conn.Delete(ctx, &result, schema.MetadataKey(key)); err != nil {
			return err
		} else if err := conn.With("key", key).Insert(ctx, &result, metadata); err != nil {
			return err
		}

		// If there are KV's, insert those as well
		for _, kv := range metadata.Metadata {
			if err := conn.With("metadata", key).Insert(ctx, nil, schema.MetadataKV{
				Key:   kv.Key,
				Value: kv.Value,
			}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return types.Ptr(result), nil
}

func (manager *Manager) ListMetadata(ctx context.Context, req schema.MetadataListRequest) (_ *schema.MetadataList, err error) {
	var result schema.MetadataList
	if err := manager.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Modify the result to include the request parameters, and clamp the count to the limit
	result.MetadataListRequest = req
	result.Clamp(result.Count)

	// Return success
	return &result, nil
}

func (manager *Manager) QueryMetadata(ctx context.Context, req schema.MetadataQueryRequest) (_ *schema.MetadataList, err error) {
	var result schema.MetadataList
	if err := manager.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Clamp count to the requested limit
	result.Clamp(result.Count)

	// Return success
	return &result, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func extractMetadataWithTimeout(ctx context.Context, extractor extractor.Extractor, path string, timeout time.Duration) ([]schema.MetadataKV, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		metadata []schema.MetadataKV
		err      error
	}

	ch := make(chan result, 1)
	go func() {
		metadata, err := extractor.ExtractMetadata(ctx, path)
		ch <- result{metadata: metadata, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		return result.metadata, result.err
	}
}
