package task

import (
	"bytes"
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) AnalyseText(ctx context.Context, object *schema.Object) error {
	var buf bytes.Buffer

	// Read the text object into a buffer
	if _, err := t.filer.WriteObject(ctx, &buf, object.Bucket, object.Key); err != nil {
		return err
	}

	// metadata
	fragment := schema.MediaFragmentMeta{
		Type: "text",
		Text: buf.String(),
	}

	// Output media update info
	ref.Log(ctx).With("fragment", fragment).Debugf(ctx, "Added text fragment")

	// Create fragment
	if _, err := t.filer.CreateMediaFragments(ctx, object.Bucket, object.Key, []schema.MediaFragmentMeta{fragment}); err != nil {
		return err
	}

	// Return sucess
	return nil
}
