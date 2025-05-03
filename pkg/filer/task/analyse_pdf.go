package task

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
	pdfapi "github.com/pdfcpu/pdfcpu/pkg/api"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) AnalysePDF(ctx context.Context, object *schema.Object) error {
	var buf bytes.Buffer

	// Read the PDF object into a buffer
	if _, err := t.filer.WriteObject(ctx, &buf, object.Bucket, object.Key); err != nil {
		return err
	}

	// Parse metadata from the PDF
	reader := bytes.NewReader(buf.Bytes())
	meta, err := parsePdfMetadata(reader)
	if err != nil {
		return err
	}

	// Get the page count
	count, err := parsePdfPageCount(reader)
	if err != nil {
		return err
	}

	// Insert the media object into the database
	media, err := t.filer.CreateMedia(ctx, object.Bucket, object.Key, schema.MediaMeta{
		Title: types.StringPtr(strings.TrimSuffix(filepath.Base(object.Key), filepath.Ext(object.Key))),
		Type:  "pdf",
		Count: types.Uint64Ptr(count),
		Meta:  meta, // Add the extracted metadata
	})
	if err != nil {
		return err
	}

	// Output media update info
	ref.Log(ctx).With("media", media).Debugf(ctx, "Updated media with PDF properties")

	// Return sucess
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func parsePdfPageCount(r io.ReadSeeker) (uint64, error) {
	// Seek to start of the file
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	// Get page count
	count, err := pdfapi.PageCount(r, nil)
	if err != nil {
		return 0, err
	}

	// Return the page count
	return uint64(count), nil
}

func parsePdfMetadata(r io.ReadSeeker) (map[string]any, error) {
	// Seek to start of the file
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Get PDF properties
	properties, err := pdfapi.Properties(r, nil) // Use default configuration
	if err != nil {
		return nil, err
	}

	// Convert properties to a map
	metaMap := make(map[string]any, len(properties))
	for k, v := range properties {
		metaMap[k] = v
	}

	// Return the metadata map
	return metaMap, nil
}
