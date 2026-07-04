package manager

import (
	"context"
	"log/slog"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (Credentials) Run(ctx context.Context, log *slog.Logger) error {
	<-ctx.Done()
	return nil
}
