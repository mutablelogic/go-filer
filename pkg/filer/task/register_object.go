package task

import (
	"context"
	"path/filepath"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterObject(ctx context.Context, object *schema.Object) error {
	var meta schema.MediaMeta
	var major, minor string

	// Set the media type
	if typ, err := types.ParseContentType(object.Type); err != nil {
		return err
	} else {
		meta.Type = typ
	}

	// Set the title
	meta.Title = types.StringPtr(strings.TrimSuffix(filepath.Base(object.Key), filepath.Ext(object.Key)))

	// If we have application/octet-stream, let's try to guess the type
	// from the file extension
	switch meta.Type {
	case types.ContentTypeBinary:
		ext := strings.ToLower(filepath.Ext(object.Key))
		switch ext {
		case ".heic":
			meta.Type = "image/heic"
		}
	}

	// Split type into major/minor
	if parts := strings.SplitN(meta.Type, "/", 2); len(parts) > 1 {
		major, minor = parts[0], parts[1]
	}

	// Insert the media object into the database
	media, err := t.filer.CreateMedia(ctx, object.Bucket, object.Key, meta)
	if err != nil {
		return err
	}

	// If the media is audio or video, create a task to analyze it
	if major == "audio" || major == "video" {
		if err := t.queueTask(ctx, TaskNameAnalyseMedia, object); err != nil {
			return err
		}
	}

	// If the media is an image, create a task to analyze its EXIF data
	if major == "image" {
		if err := t.queueTask(ctx, TaskNameAnalyseImageEXIF, object); err != nil {
			return err
		}
	}

	// PDF
	if major == "application" && minor == "pdf" {
		if err := t.queueTask(ctx, TaskNameAnalysePDF, object); err != nil {
			return err
		}
	}

	// Plain text
	if major == "text" && minor == "plain" {
		if err := t.queueTask(ctx, TaskNameAnalyseText, object); err != nil {
			return err
		}
	}

	// HTML
	if major == "text" && minor == "html" {
		if err := t.queueTask(ctx, TaskNameAnalyseHTML, object); err != nil {
			return err
		}
	}

	ref.Log(ctx).With("media", media).Debugf(ctx, "Created media object")

	// Return sucess
	return nil
}
