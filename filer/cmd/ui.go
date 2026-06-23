//go:build !noui && !client

package cmd

import (
	"context"
	"io/fs"
	"log/slog"

	ts "github.com/mutablelogic/go-filer/build/ts"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

type UIFlags struct {
	UI bool `long:"ui" help:"Serve the web UI from this server" default:"true" negatable:""`
}

func (f *UIFlags) MaybeRegisterUI(ctx context.Context, log *slog.Logger, router *httprouter.Router) error {
	if !f.UI {
		return nil
	}
	log.DebugContext(ctx, "registering ui handlers")
	sub, err := fs.Sub(ts.EmbedFS, "dist")
	if err != nil {
		return err
	}
	return router.RegisterFS("/", sub, false, nil)
}
