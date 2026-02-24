package main

import (
	"encoding/json"
	"os"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BackendsCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *BackendsCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	resp, err := c.ListBackends(ctx.ctx)
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
