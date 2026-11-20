package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

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
	ActionList ActionListCmd `cmd:"" name:"actions" help:"List the actions of a volume, or of every volume, which their backends can do beyond reading and writing their objects." group:"ACTION"`
	ActionCall ActionCallCmd `cmd:"" name:"action" help:"Call an action of a volume, with its input, of JSON, and show the object it created or changed." group:"ACTION"`
}

type ActionListCmd struct {
	Name string `arg:"" name:"name" optional:"" help:"Volume name, or every volume which is mounted, and online, without it"`
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
		table := tui.TableFor[actionRow](tui.SetWidth(width))
		_, err = table.Write(os.Stdout, actionRows(list.Body)...)
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

///////////////////////////////////////////////////////////////////////////////
// TABLE

// actionRow is an action of one or more volumes, which have the same
// action, of its name, title and description, so it's listed once
type actionRow struct {
	schema.Action
	volumes []string
}

// actionRows returns the rows of the actions, in order of their names, of
// those which are the same, of their volumes, in order
func actionRows(actions []schema.Action) []actionRow {
	var rows []actionRow
	index := make(map[[3]string]int)
	for _, action := range actions {
		key := [3]string{action.Name, action.Title, action.Description}
		if i, exists := index[key]; exists {
			rows[i].volumes = append(rows[i].volumes, action.Volume)
			continue
		}
		index[key] = len(rows)
		rows = append(rows, actionRow{Action: action, volumes: []string{action.Volume}})
	}
	slices.SortStableFunc(rows, func(a, b actionRow) int { return strings.Compare(a.Name, b.Name) })
	for i := range rows {
		slices.Sort(rows[i].volumes)
	}
	return rows
}

func (r actionRow) Header() []string {
	return []string{"Name", "Title", "Description", "Volumes"}
}

func (r actionRow) Width(col int) int {
	return 0
}

func (r actionRow) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return r.Name
	case 1:
		return r.Title
	case 2:
		return r.Description
	case 3:
		return strings.Join(r.volumes, ", ")
	default:
		return ""
	}
}
