package task

import (
	"bytes"
	"context"
	"errors"
	"image"
	"path/filepath"
	"strings"
	"time"

	// Packages
	exif "github.com/dsoprea/go-exif/v3"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	EXIFKeyDateTime    = "DateTime"
	EXIFKeyWidth       = "PixelXDimension"
	EXIFKeyHeight      = "PixelYDimension"
	EXIFDateTimeFormat = "2006:01:02 15:04:05"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) AnalyseImageEXIF(ctx context.Context, object *schema.Object) error {
	var buf bytes.Buffer

	// Default metadata
	meta := schema.MediaMeta{
		Title: types.StringPtr(strings.TrimSuffix(filepath.Base(object.Key), filepath.Ext(object.Key))),
		Type:  "photo",
	}

	// Let's read the object into the buffer
	if _, err := t.filer.WriteObject(ctx, &buf, object.Bucket, object.Key); err != nil {
		return err
	}

	// Attempt to read the width and height from the image
	config, typ, err := image.DecodeConfig(&buf)
	if err == nil {
		meta.Width = types.Uint64Ptr(uint64(config.Width))
		meta.Height = types.Uint64Ptr(uint64(config.Height))
		meta.Type = typ
	}

	// Parse the EXIF data
	if err := parseExifData(buf.Bytes(), &meta); err != nil {
		return err
	}

	// Insert the media object into the database
	media, err := t.filer.CreateMedia(ctx, object.Bucket, object.Key, meta)
	if err != nil {
		return err
	}

	// Output media
	ref.Log(ctx).With("media", media).Debugf(ctx, "Created media")

	// Return sucess
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func parseExifData(data []byte, meta *schema.MediaMeta) error {
	// Parse the EXIF data
	raw, err := exif.SearchAndExtractExif(data)
	if errors.Is(err, exif.ErrNoExif) {
		return nil
	} else if err != nil {
		return err
	}

	// Flatten the EXIF data
	entries, _, err := exif.GetFlatExifData(raw, new(exif.ScanOptions))
	if err != nil {
		return err
	}

	// Create a map of the EXIF data
	meta.Meta = make(map[string]any, len(entries))
	for _, entry := range entries {
		meta.Meta[entry.TagName] = entry.Value
	}

	// Parse the DateTime into timestamp
	if dateTime, ok := meta.Meta[EXIFKeyDateTime].(string); ok && dateTime != "" {
		if t, err := time.Parse(EXIFDateTimeFormat, dateTime); err == nil {
			meta.Ts = types.TimePtr(t.UTC())
		}
	}

	// Parse the width and height from EXIF data
	if w, ok := meta.Meta[EXIFKeyWidth].([]uint32); ok && len(w) == 1 {
		meta.Width = types.Uint64Ptr(uint64(w[0]))
	}
	if h, ok := meta.Meta[EXIFKeyWidth].([]uint32); ok && len(h) == 1 {
		meta.Height = types.Uint64Ptr(uint64(h[0]))
	}

	// Return success
	return nil
}
