package task

import (
	"context"
	"fmt"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
)

func (t *taskrunner) RegisterObject(ctx context.Context, in *schema.Object) error {
	// If the object type is audio or video, then let's get the metadata
	fmt.Println("RegisterObject called: ", in)
	return nil
}
