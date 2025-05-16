package cmd

import (
	"context"
	"fmt"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/llm/client"
	schema "github.com/mutablelogic/go-filer/pkg/llm/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ModelCommands struct {
	Models ModelListCommand `cmd:"" group:"LLM" help:"List models"`
	Model  ModelGetCommand  `cmd:"get" group:"LLM" help:"Get model"`
}

type ModelListCommand struct {
	schema.ModelListRequest
}

type ModelGetCommand struct {
	Name string `arg:"" help:"Model name"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ModelListCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, llm *client.Client) error {
		models, err := llm.ListModels(ctx, client.WithOffsetLimit(cmd.Offset, cmd.Limit))
		if err != nil {
			return err
		}
		fmt.Println(models)
		return nil
	})
}

func (cmd *ModelGetCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, llm *client.Client) error {
		model, err := llm.GetModel(ctx, cmd.Name)
		if err != nil {
			return err
		}
		fmt.Println(model)
		return nil
	})
}
