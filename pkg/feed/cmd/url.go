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
	Url       UrlGetCommand    `cmd:"" group:"FEED" help:"Get a feed url"`
	CreateUrl UrlCreateCommand `cmd:"" group:"FEED" help:"Create a new url"`
	UpdateUrl UrlUpdateCommand `cmd:"" group:"FEED" help:"Update a feed url"`
	DeleteUrl UrlDeleteCommand `cmd:"" group:"FEED" help:"Delete a feed url"`
}

type UrlListCommand struct {
	schema.UrlListRequest
}

type UrlCreateCommand struct {
	Url string `arg:"" help:"URL to create"`
	schema.UrlMeta
}

type UrlGetCommand struct {
	Id string `arg:"" help:"ID of Url"`
}

type UrlDeleteCommand struct {
	UrlGetCommand
}

type UrlUpdateCommand struct {
	UrlGetCommand
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
		url, err := feed.CreateUrl(ctx, cmd.Url, cmd.UrlMeta)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	})
}

func (cmd *UrlGetCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, feed *client.Client) error {
		url, err := feed.GetUrl(ctx, cmd.Id)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	})
}

func (cmd *UrlDeleteCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, feed *client.Client) error {
		return feed.DeleteUrl(ctx, cmd.Id)
	})
}

func (cmd *UrlUpdateCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, feed *client.Client) error {
		url, err := feed.UpdateUrl(ctx, cmd.Id, cmd.UrlMeta)
		if err != nil {
			return err
		}
		fmt.Println(url)
		return nil
	})
}
