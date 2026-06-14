package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	manager "github.com/mutablelogic/go-filer/extractor/manager"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	pgcmd "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"

	// Extractors
	_ "github.com/mutablelogic/go-filer/extractor/audio"
	_ "github.com/mutablelogic/go-filer/extractor/image"
	_ "github.com/mutablelogic/go-filer/extractor/markdown"
	_ "github.com/mutablelogic/go-filer/extractor/pdf"
	_ "github.com/mutablelogic/go-filer/extractor/srt"
	_ "github.com/mutablelogic/go-filer/extractor/text"
	_ "github.com/mutablelogic/go-filer/extractor/video"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Commands struct {
	Index  IndexCommand  `cmd:"" name:"index" help:"Index metadata from a file." group:"EXTRACT"`
	List   ListCommand   `cmd:"" name:"list" help:"List metadata for indexed files." group:"EXTRACT"`
	Search SearchCommand `cmd:"" name:"search" help:"Search metadata for indexed files." group:"EXTRACT"`
}

type Common struct {
	pgcmd.PostgresFlags
}

type IndexCommand struct {
	Common
	Path      string `arg:"" name:"path" type:"file" help:"Path to the file to index metadata from."`
	Recursive bool   `flag:"" short:"r" name:"recursive" help:"Recursively index metadata from files in a directory." default:"false"`
	Force     bool   `flag:"" short:"f" name:"force" help:"Force re-indexing of metadata, even if the file has not changed." default:"false"`
}

type ListCommand struct {
	Common
	schema.MetadataListRequest
}

type SearchCommand struct {
	Common
	schema.MetadataQueryRequest
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c IndexCommand) Run(ctx server.Cmd) error {
	return c.WithManager(ctx, func(manager *manager.Manager) error {
		ctx.Logger().Debug("Starting extractor", "name", ctx.Name(), "version", ctx.Version())

		// Walk the path and extract metadata from each file, and insert that into the database
		return filepath.Walk(c.Path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.HasPrefix(filepath.Base(path), ".") {
				// Skip hidden files and directories
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if info.IsDir() {
				if path != c.Path && !c.Recursive {
					return filepath.SkipDir
				}
				return nil
			}

			// We use the path as the key
			var warn error
			start := time.Now()
			if indexed, err := manager.IndexFileAtPath(ctx.Context(), filepath.Clean(path), filepath.Clean(path), info, c.Force, &warn); err != nil {
				return err
			} else if errors.Is(warn, gofiler.ErrNotIndexed) {
				ctx.Logger().Debug("Not modified", "path", path)
			} else if err != nil {
				ctx.Logger().Warn("Indexing error", "path", path, "error", warn.Error(), "elapsed", time.Since(start).Truncate(time.Millisecond).String())
			} else {
				ctx.Logger().Debug("Indexed", "path", path, "elapsed", time.Since(start).Truncate(time.Millisecond).String(), "metadata", indexed)
			}
			return nil
		})
	})
}

func (c ListCommand) Run(ctx server.Cmd) error {
	return c.WithManager(ctx, func(manager *manager.Manager) error {
		ctx.Logger().Debug("Listing metadata", "name", ctx.Name(), "version", ctx.Version())

		metadata, err := manager.ListMetadata(ctx.Context(), c.MetadataListRequest)
		if err != nil {
			return err
		}

		// Metadata list table
		table := tui.TableFor[*schema.Metadata](tui.SetWidth(ctx.IsTerm()))
		if _, err := table.Write(os.Stdout, metadata.Body...); err != nil {
			return err
		}

		// Metadata list summary
		summary := tui.TableSummary("metadata items", uint(metadata.Count), metadata.Offset, metadata.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (c SearchCommand) Run(ctx server.Cmd) error {
	return c.WithManager(ctx, func(manager *manager.Manager) error {
		ctx.Logger().Debug("Searching metadata", "name", ctx.Name(), "version", ctx.Version())

		metadata, err := manager.QueryMetadata(ctx.Context(), c.MetadataQueryRequest)
		if err != nil {
			return err
		}

		// Metadata list table
		table := tui.TableFor[*schema.Metadata](tui.SetWidth(ctx.IsTerm()))
		if _, err := table.Write(os.Stdout, metadata.Body...); err != nil {
			return err
		}

		// Metadata list summary
		summary := tui.TableSummary("metadata items", uint(metadata.Count), metadata.Offset, metadata.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (c Common) WithManager(ctx server.Cmd, fn func(*manager.Manager) error) error {
	conn, err := c.PostgresFlags.Connect(ctx)
	if err != nil {
		return err
	} else if conn == nil {
		return fmt.Errorf("failed to connect to postgres")
	}

	manager, err := manager.New(ctx.Context(), conn)
	if err != nil {
		return err
	}

	return fn(manager)
}
