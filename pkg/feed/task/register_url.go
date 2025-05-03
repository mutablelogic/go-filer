package task

import (
	"context"
	"fmt"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterUrl(ctx context.Context, url *schema.Url) error {
	fmt.Println("TODO: RegisterUrl", url)

	// Return sucess
	return nil
}
