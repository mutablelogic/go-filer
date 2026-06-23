//go:build noui || client

package cmd

import (
	"context"
	"log/slog"

	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

type UIFlags struct{}

func (f *UIFlags) MaybeRegisterUI(_ context.Context, _ *slog.Logger, _ *httprouter.Router) error {
	return nil
}
