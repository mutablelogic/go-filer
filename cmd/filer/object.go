package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ListCommand struct {
	Backend   string `arg:"" name:"backend" help:"Backend name"`
	Path      string `name:"path" help:"Path prefix to list" default:"/"`
	Recursive bool   `name:"recursive" help:"List recursively"`
}

type GetCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Object path (e.g. /dir/file.txt)"`
	Output  string `name:"output" short:"o" help:"Write to file instead of stdout"`
}

type HeadCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Object path"`
}

type CreateCommand struct {
	Backend     string   `arg:"" name:"backend" help:"Backend name"`
	Path        string   `arg:"" name:"path" help:"Object path"`
	File        string   `name:"file" short:"f" help:"Local file to upload (defaults to stdin)"`
	ContentType string   `name:"type" short:"t" help:"Content-Type (e.g. text/plain)"`
	Meta        []string `name:"meta" help:"Metadata as key=value pairs (repeatable)"`
}

type DeleteCommand struct {
	Backend   string `arg:"" name:"backend" help:"Backend name"`
	Path      string `arg:"" name:"path" help:"Object path or prefix"`
	Recursive bool   `name:"recursive" help:"Delete all objects under path"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ListCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	resp, err := c.ListObjects(ctx.ctx, cmd.Backend, schema.ListObjectsRequest{
		Path:      cmd.Path,
		Recursive: cmd.Recursive,
	})
	if err != nil {
		return err
	}
	return prettyJSON(resp)
}

func (cmd *HeadCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	obj, err := c.GetObject(ctx.ctx, cmd.Backend, schema.GetObjectRequest{
		Path: cmd.Path,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
}

func (cmd *GetCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	reader, _, err := c.ReadObject(ctx.ctx, cmd.Backend, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: cmd.Path},
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	var out io.Writer = os.Stdout
	if cmd.Output != "" {
		f, err := os.Create(cmd.Output)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	_, err = io.Copy(out, reader)
	return err
}

func (cmd *CreateCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}

	var src io.Reader = os.Stdin
	if cmd.File != "" {
		f, err := os.Open(cmd.File)
		if err != nil {
			return err
		}
		defer f.Close()
		src = f
	}

	var meta schema.ObjectMeta
	for _, kv := range cmd.Meta {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("invalid meta %q: expected key=value", kv)
		}
		if meta == nil {
			meta = make(schema.ObjectMeta)
		}
		meta[k] = v
	}

	obj, err := c.CreateObject(ctx.ctx, cmd.Backend, schema.CreateObjectRequest{
		Path:        cmd.Path,
		Body:        src,
		ContentType: cmd.ContentType,
		Meta:        meta,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
}

func (cmd *DeleteCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	if cmd.Recursive {
		resp, err := c.DeleteObjects(ctx.ctx, cmd.Backend, schema.DeleteObjectsRequest{
			Path:      cmd.Path,
			Recursive: true,
		})
		if err != nil {
			return err
		}
		return prettyJSON(resp)
	}
	obj, err := c.DeleteObject(ctx.ctx, cmd.Backend, schema.DeleteObjectRequest{
		Path: cmd.Path,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
}
