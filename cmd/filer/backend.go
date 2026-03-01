package main

import (
	"encoding/json"
	"os"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BackendsCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *BackendsCommand) Run(ctx *Globals) (err error) {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	cmdCtx, endSpan := otel.StartSpan(ctx.tracer, ctx.ctx, "filer.cli.Backends")
	defer func() { endSpan(err) }()
	resp, err := c.ListBackends(cmdCtx)
	if err != nil {
		return err
	}
	return prettyJSON(resp)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func prettyJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
