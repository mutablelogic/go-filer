package cmd

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	// Packages
	kong "github.com/alecthomas/kong"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// SearchCommands are the commands which search the objects of volumes
type SearchCommands struct {
	Search SearchCmd `cmd:"" name:"search" help:"Search the objects of the mounted volumes, of their names, folders and metadata, best first, a page at a time." group:"OBJECT"`
}

type SearchCmd struct {
	Query   []string `arg:"" name:"query" passthrough:"" help:"Words, any of which match, quoted phrases, the starts of words, ending with *, and words which don't match, starting with -, such as arvo '\"spiegel im spiegel\"' beethov* -symphony, after any flags"`
	Offset  uint64   `name:"offset" help:"How many of the objects which match are skipped, before those which are shown"`
	Limit   *uint64  `name:"limit" help:"Maximum number of objects to show, which defaults to and is capped at 100"`
	NoTitle bool     `name:"no-title" help:"Show the paths of objects, rather than their titles"`
}

// searched is an object which matches a search, as a row of a table
type searched struct {
	*schema.SearchObject
	titles bool // Whether its title is shown, rather than its path
}

///////////////////////////////////////////////////////////////////////////////
// HOOKS

// AfterApply sets the query to the words of the command line as they're
// written, as Kong splits a word which starts with -, which is excluded, as
// short flags, when it's the first of the query, such as -symphony, of -s
// and ymphony, which are its characters, so the query is the fewest words,
// at the end of the command line, which are of the same characters, without
// a -- before them
func (cmd *SearchCmd) AfterApply(kctx *kong.Context) error {
	cmd.Query = rawQuery(kctx.Args, cmd.Query)
	if len(cmd.Query) > 0 && cmd.Query[0] == "--" {
		// Kong keeps --, before the words of a passthrough argument
		cmd.Query = cmd.Query[1:]
	}
	return nil
}

// rawQuery returns the words at the end of the arguments which are of the
// characters of the query which Kong parsed, or the query, when they aren't
func rawQuery(args, query []string) []string {
	want := strings.Join(query, "")
	if want == "" {
		return query
	}
	got := ""
	for i := len(args) - 1; i >= 0; i-- {
		if got = args[i] + got; got == want {
			return slices.Clone(args[i:])
		} else if len(got) > len(want) {
			break
		}
	}
	return query
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *SearchCmd) Run(ctx server.Cmd) error {
	width := ctx.IsTerm()
	debug := ctx.IsDebug()
	return withClient(ctx, "filer.SearchCmd", func(ctx context.Context, client *httpclient.Client) error {
		result, err := client.Search(ctx, strings.Join(cmd.Query, " "), schema.OffsetLimit{Offset: cmd.Offset, Limit: cmd.Limit})
		if err != nil {
			return err
		}
		if debug {
			fmt.Println(result)
			return nil
		}
		rows := make([]searched, 0, len(result.Body))
		for _, object := range result.Body {
			rows = append(rows, searched{SearchObject: object, titles: !cmd.NoTitle})
		}
		table := tui.TableFor[searched](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, rows...); err != nil {
			return err
		}
		_, err = tui.TableSummary("objects", uint(result.Count), uint(len(result.Body)), result.Offset, result.Limit).Write(os.Stdout)
		return err
	})
}

///////////////////////////////////////////////////////////////////////////////
// TABLE

func (o searched) Header() []string {
	path := "Path"
	if o.titles {
		path = "Title"
	}
	return []string{"Volume", path, "Type", "Rank"}
}

func (o searched) Width(int) int {
	return 0
}

func (o searched) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return o.Name
	case 1:
		if title := objectTitle(&o.Object); o.titles && title != "" {
			return title
		}
		return o.Key()
	case 2:
		if o.IsDir {
			return ""
		}
		return o.ContentType
	default:
		return strconv.FormatFloat(float64(o.Rank), 'f', 2, 32)
	}
}
