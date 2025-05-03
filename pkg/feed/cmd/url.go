package cmd

import (
	"context"
	"fmt"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/feed/client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type UrlCommands struct {
	Urls      UrlListCommand   `cmd:"" group:"FEED" help:"List feed urls"`
	CreateUrl UrlCreateCommand `cmd:"" group:"FEED" help:"Create a new url"`
}

type UrlListCommand struct {
	schema.UrlListRequest
}

type UrlCreateCommand struct {
	schema.UrlMeta
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *UrlListCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, feed *client.Client) error {
		urls, err := feed.ListUrls(ctx, client.WithOffsetLimit(cmd.Offset, cmd.Limit))
		if err != nil {
			return err
		}
		fmt.Println(urls)
		return nil
	})
}

func (cmd *UrlCreateCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, feed *client.Client) error {
		url, err := feed.CreateUrl(ctx, cmd.UrlMeta)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	})
}
