package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mutablelogic/go-client/pkg/otel"
	"github.com/mutablelogic/go-filer/filer/httpclient"
	"github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	ObjectClientCommands
	VolumeClientCommands
	MetadataClientCommands
}

type ObjectClientCommands struct {
	ObjectList ObjectListCmd `cmd:"" name:"objects" help:"List server objects." group:"OBJECT"`
}

type VolumeClientCommands struct {
	VolumeList   VolumeListCmd   `cmd:"" name:"volumes" help:"List server volumes." group:"VOLUME"`
	VolumeCreate VolumeCreateCmd `cmd:"" name:"volume-create" help:"Create a new volume." group:"VOLUME"`
}

type MetadataClientCommands struct {
	Metadata MetadataCmd `cmd:"" name:"metadata" help:"Extract metadata for a file using the server endpoint." group:"METADATA"`
}

type ObjectListCmd struct {
	schema.ObjectListRequest
}

type VolumeCreateCmd struct {
	schema.VolumeCreate
}

type VolumeListCmd struct {
	schema.VolumeListRequest
}

type MetadataCmd struct {
	Path string `arg:"" name:"path" type:"file" help:"Path to the local file."`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func withClient(ctx server.Cmd, span string, fn func(context.Context, *httpclient.Client) error) error {
	endpoint, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	} else if client, err := httpclient.New(endpoint, opts...); err != nil {
		return err
	} else {
		var err error
		ctx, endfn := otel.StartSpan(ctx.Tracer(), ctx.Context(), span)
		defer func() { endfn(err) }()
		err = fn(ctx, client)
		return err
	}
}

///////////////////////////////////////////////////////////////////////////////
// OBJECT COMMANDS

func (cmd *ObjectListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "objects", func(ctx context.Context, client *httpclient.Client) error {
		objects, err := client.ListObjects(ctx, cmd.ObjectListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(objects)
			return nil
		}

		// Objects list table
		table := tui.TableFor[*schema.Object](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, objects.Body...); err != nil {
			return err
		}

		// Objects list summary
		summary := tui.TableSummary("objects", uint(objects.Count), objects.Offset, objects.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// VOLUME COMMANDS

func (cmd *VolumeListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "volumes", func(ctx context.Context, client *httpclient.Client) error {
		volumes, err := client.ListVolumes(ctx, cmd.VolumeListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(volumes)
			return nil
		}

		// Volumes list table
		table := tui.TableFor[*schema.Volume](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, volumes.Body...); err != nil {
			return err
		}

		// Volumes list summary
		summary := tui.TableSummary("volumes", uint(volumes.Count), volumes.Offset, volumes.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *VolumeCreateCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume-create", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.CreateVolume(ctx, cmd.VolumeCreate)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// METADATA COMMANDS

func (cmd *MetadataCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "metadata", func(ctx context.Context, client *httpclient.Client) error {
		f, err := os.Open(cmd.Path)
		if err != nil {
			return err
		}
		defer f.Close()

		meta, err := client.GetMetadata(ctx, f)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(meta)
	})
}
