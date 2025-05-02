package cmd

import (
	"context"
	"fmt"
	"time"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/filer/client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectCommands struct {
	UploadObjects CreateObjectsCommand `cmd:"" group:"FILER" help:"Upload files"`
	Objects       ListObjectsCommand   `cmd:"" group:"FILER" help:"List objects"`
	Object        GetObjectCommand     `cmd:"" group:"FILER" help:"Get object metadata"`
	DeleteObject  DeleteObjectCommand  `cmd:"" group:"FILER" help:"Delete object"`
}

type CreateObjectsCommand struct {
	Bucket string   `arg:"" name:"bucket" help:"Name of the bucket"`
	Path   []string `arg:"" type:"path" help:"File or path of files to upload"`
}

type ListObjectsCommand struct {
	Bucket string `arg:"" name:"bucket" help:"Name of the bucket"`
	schema.ObjectListRequest
}

type GetObjectCommand struct {
	Bucket string `arg:"" name:"bucket" help:"Name of the bucket"`
	Key    string `arg:"" name:"key" help:"Object key"`
}

type DeleteObjectCommand struct {
	GetObjectCommand
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *CreateObjectsCommand) Run(app server.Cmd) error {
	now := time.Now()
	return run(app, func(ctx context.Context, filer *client.Client) error {
		response, err := filer.CreateObjects(ctx, cmd.Bucket, cmd.Path, client.WithProgress(func(cur, total uint64) {
			if time.Since(now) > time.Second {
				fmt.Printf("Uploaded %v/%v bytes (%.1f%%)\r", cur, total, float64(cur)/float64(total)*100)
				now = time.Now()
			}
		}))
		if err != nil {
			return err
		}

		if len(response.Body) == 0 {
			fmt.Print("No objects created")
		} else {
			fmt.Println(response.Body)
			fmt.Println("Total objects created:", len(response.Body))
		}
		return nil
	})
}

func (cmd *ListObjectsCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		response, err := filer.ListObjects(ctx, cmd.Bucket, client.WithPrefix(cmd.Prefix))
		if err != nil {
			return err
		}

		fmt.Println(response)
		return nil
	})
}

func (cmd *DeleteObjectCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		return filer.DeleteObject(ctx, cmd.Bucket, cmd.Key)
	})
}

func (cmd *GetObjectCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		object, err := filer.GetObject(ctx, cmd.Bucket, cmd.Key)
		if err != nil {
			return err
		}
		fmt.Println(object)
		return nil
	})
}
