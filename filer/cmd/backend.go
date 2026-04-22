package cmd

import (
	"context"
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BackendsCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *BackendsCommand) Run(ctx server.Cmd) (err error) {
	return withClient(ctx, "BackEnds", nil, func(ctx context.Context, client *httpclient.Client) error {
		resp, err := client.ListBackends(ctx)
		if err != nil {
			return err
		}
		fmt.Println(types.Stringify(resp))
		return nil
	})
}
