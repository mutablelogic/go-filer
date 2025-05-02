package task

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterObject(ctx context.Context, object *schema.Object) error {
	var wg sync.WaitGroup
	var errs error

	// Make a pipe
	pr, pw := io.Pipe()
	defer wg.Wait()
	defer pr.Close()

	// Let's read the object in the background
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pw.Close()
		if _, err := t.filer.WriteObject(ctx, pw, object.Bucket, object.Key); err != nil {
			errs = errors.Join(errs, err)
		}
	}()

	// Let's analyze the object in the foreground
	media, err := ffmpeg.NewReader(pr)
	if err != nil {
		errs = errors.Join(errs, err)
	} else if media != nil {
		errs = errors.Join(errs, t.registerObjectMedia(ctx, object, media))
		media.Close()
	}

	// Return any errors
	return errs
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t *taskrunner) registerObjectMedia(ctx context.Context, object *schema.Object, media *ffmpeg.Reader) error {
	fmt.Println(media)
	for _, metadata := range media.Metadata() {
		log.Print(metadata.Key(), " => ", metadata.Value())
	}

	// Retrieve artwork by using the MetaArtwork key. The value is of type []byte.
	// which needs to be converted to an image.
	for _, artwork := range media.Metadata(ffmpeg.MetaArtwork) {
		mimetype := artwork.Value()
		if mimetype != "" {
			// Retrieve the data using the metadata.Bytes() method
			log.Print("We got some artwork of mimetype ", mimetype)
		}
	}

	// Return success
	return nil
}
