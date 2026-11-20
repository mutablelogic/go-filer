package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ActionCommands struct {
	ActionList ActionListCmd `cmd:"" name:"actions" help:"List the actions of a volume, which its backend can do beyond reading and writing its objects." group:"VOLUME"`
	ActionCall ActionCallCmd `cmd:"" name:"action" help:"Call an action of a volume, with its input, of JSON, and show the object it created or changed." group:"VOLUME"`
}

type ActionListCmd struct {
	Name string `arg:"" name:"name" help:"Volume name"`
}

type ActionCallCmd struct {
	Name   string `arg:"" name:"name" help:"Volume name"`
	Action string `arg:"" name:"action" help:"Action name, such as task_list"`
	Input  string `arg:"" name:"input" optional:"" help:"Input of the action, of JSON, such as '{\"op\":\"create\",\"title\":\"Work\"}', or - to read it from the standard input. Defaults to {}."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ActionListCmd) Run(ctx server.Cmd) error {
	width := ctx.IsTerm()
	debug := ctx.IsDebug()
	return withClient(ctx, "filer.ActionListCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListActions(ctx, cmd.Name)
		if err != nil {
			return err
		} else if debug {
			fmt.Println(list)
			return nil
		}
		table := tui.TableFor[schema.Action](tui.SetWidth(width))
		_, err = table.Write(os.Stdout, list.Body...)
		return err
	})
}

func (cmd *ActionCallCmd) Run(ctx server.Cmd) error {
	input, err := cmd.input(os.Stdin)
	if err != nil {
		return err
	}
	return withClient(ctx, "filer.ActionCallCmd", func(ctx context.Context, client *httpclient.Client) error {
		object, err := client.CallAction(ctx, cmd.Name, cmd.Action, input)
		if err != nil {
			return err
		}
		fmt.Println(object)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// input returns the action's input, of its argument, or of the reader, when
// it's -, which is JSON, or {} when it's empty
func (cmd *ActionCallCmd) input(r io.Reader) (json.RawMessage, error) {
	if !gofiler.ValidActionName(cmd.Action) {
		return nil, gofiler.ErrBadParameter.Withf("invalid action name: %q", cmd.Action)
	}
	data := []byte(cmd.Input)
	if cmd.Input == "-" {
		var err error
		if data, err = io.ReadAll(r); err != nil {
			return nil, err
		}
	}
	if len(data) == 0 {
		return json.RawMessage("{}"), nil
	} else if !json.Valid(data) {
		return nil, gofiler.ErrBadParameter.Withf("the input of action %q isn't JSON", cmd.Action)
	}
	return json.RawMessage(data), nil
}
