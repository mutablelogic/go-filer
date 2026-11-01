package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeCommands struct {
	VolumeList    VolumeListCmd    `cmd:"" name:"volumes" help:"List volumes, with the URLs of their backends." group:"VOLUME"`
	VolumeGet     VolumeGetCmd     `cmd:"" name:"volume" help:"Get a volume by its name, with the URL of its backend." group:"VOLUME"`
	VolumeSchemes VolumeSchemesCmd `cmd:"" name:"volume-schemes" help:"List the schemes of the URLs which volumes can be created from." group:"VOLUME"`
	VolumeCreate  VolumeCreateCmd  `cmd:"" name:"volume-create" help:"Create a volume from the URL of its backend, or a directory." group:"VOLUME"`
	VolumeUpdate  VolumeUpdateCmd  `cmd:"" name:"volume-update" help:"Change the options of a volume by its name." group:"VOLUME"`
	VolumeDelete  VolumeDeleteCmd  `cmd:"" name:"volume-delete" help:"Delete a volume by its name, without deleting its files." group:"VOLUME"`
	VolumeWatch   VolumeWatchCmd   `cmd:"" name:"volume-watch" help:"Show the changes to the objects of volumes, as they happen, until it's interrupted." group:"VOLUME"`
}

type VolumeListCmd struct{}

type VolumeSchemesCmd struct{}

type VolumeGetCmd struct {
	Name string `arg:"" name:"name" help:"Volume name."`
}

type VolumeUpdateCmd struct {
	VolumeGetCmd
	Opt   []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Option to set, such as readonly=true, which can be repeated, when the option has each of the values, such as -o skip=^\\. -o skip=\\.tmp$. An option without a value, such as skip, has none."`
	Unset []string `name:"unset" sep:"none" placeholder:"KEY" help:"Option to remove, so it's the default, which can be repeated."`
}

type VolumeWatchCmd struct {
	Name []string `arg:"" name:"name" optional:"" help:"Volumes to watch, which is every volume without any."`
}

type VolumeDeleteCmd struct {
	VolumeGetCmd
}

type VolumeCreateCmd struct {
	URL  string   `arg:"" name:"url" help:"URL of the volume's backend, such as file://media/mnt/folder, whose host is the volume's name, or a directory on the server, such as /mnt/media, with --name, or a bucket, such as s3://bucket/prefix?name=photos."`
	Name string   `name:"name" help:"Volume name, which is an identifier. For a file URL it's the host, and otherwise the name option, or the host, which it defaults to, and sets the option when it isn't the host."`
	Opt  []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Option of the volume, such as readonly=true, skip=^\\. or package=\\.bundle$, which can be repeated, and is added to those of the URL. An option without a value, such as readonly, is empty, which for readonly is true."`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *VolumeListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "filer.VolumeListCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListVolumes(ctx)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(list)
			return nil
		}

		// Volumes table
		table := tui.TableFor[schema.Volume](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, list.Body...); err != nil {
			return err
		}

		// Volumes summary, which are all the volumes
		count := uint(len(list.Body))
		if _, err := tui.TableSummary("volumes", count, count, 0, nil).Write(os.Stdout); err != nil {
			return err
		}
		return nil
	})
}

func (cmd *VolumeSchemesCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "filer.VolumeSchemesCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListVolumes(ctx)
		if err != nil {
			return err
		}

		// The schemes, one on each line
		for _, scheme := range list.Schemes {
			fmt.Println(scheme)
		}
		return nil
	})
}

func (cmd *VolumeCreateCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	return withClient(ctx, "filer.VolumeCreateCmd", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.CreateVolume(ctx, volume)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "filer.VolumeGetCmd", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.GetVolume(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeUpdateCmd) Run(ctx server.Cmd) error {
	patch, err := cmd.patch()
	if err != nil {
		return err
	}
	return withClient(ctx, "filer.VolumeUpdateCmd", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.UpdateVolume(ctx, cmd.Name, patch)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeWatchCmd) Run(ctx server.Cmd) error {
	debug := ctx.IsDebug()
	return withClient(ctx, "filer.VolumeWatchCmd", func(ctx context.Context, client *httpclient.Client) error {
		return client.WatchObjects(ctx, cmd.Name, func(event schema.Event) error {
			if debug {
				fmt.Println(types.Stringify(event))
			} else {
				fmt.Printf("%s %-8s filer://%s%s\n", time.Now().Format(time.TimeOnly), event.EventType, event.Name, event.Path)
			}
			return nil
		})
	})
}

func (cmd *VolumeDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "filer.VolumeDeleteCmd", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.DeleteVolume(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// patch returns the options to change: each option to set has the values it
// was given, where one without a value has none, and each option to remove
// is null. Returns ErrBadParameter when an option is set and removed, or has
// no key, or there are no options.
func (cmd *VolumeUpdateCmd) patch() (schema.VolumePatch, error) {
	values := map[string][]string{}
	for _, opt := range cmd.Opt {
		key, value, hasValue := strings.Cut(opt, "=")
		if key = strings.TrimSpace(key); key == "" {
			return nil, gofiler.ErrBadParameter.Withf("invalid option: %q", opt)
		} else if _, exists := values[key]; !exists {
			values[key] = []string{}
		}
		if hasValue {
			values[key] = append(values[key], value)
		}
	}
	patch := make(schema.VolumePatch, len(values)+len(cmd.Unset))
	for key, values := range values {
		data, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		patch[key] = data
	}
	for _, key := range cmd.Unset {
		if key = strings.TrimSpace(key); key == "" {
			return nil, gofiler.ErrBadParameter.With("invalid option to remove: \"\"")
		} else if _, exists := values[key]; exists {
			return nil, gofiler.ErrBadParameter.Withf("option %q is set and removed", key)
		}
		patch[key] = json.RawMessage("null")
	}
	if len(patch) == 0 {
		return nil, gofiler.ErrBadParameter.With("no options to update, with --opt or --unset")
	}
	return patch, nil
}

// volume returns the volume to create. The name is the URL's host, unless
// it's set, when a URL without a host has the name as its host, so
// file:///mnt/media is file://<name>/mnt/media, and a URL
// without a scheme is a directory, with the options after a "?", such as
// /mnt/media?readonly=true, which is made absolute, as the server's
// directory, so it's best given as one which is absolute.
func (cmd *VolumeCreateCmd) volume() (schema.Volume, error) {
	u, err := url.Parse(cmd.URL)
	if err != nil || u.Scheme == "" || filepath.IsAbs(cmd.URL) {
		dir, query, _ := strings.Cut(cmd.URL, "?")
		path, err := filepath.Abs(dir)
		if err != nil {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid url: %q", cmd.URL)
		}
		u = &url.URL{Scheme: schema.SchemeFile, Path: filepath.ToSlash(path), RawQuery: query}
		if !strings.HasPrefix(u.Path, "/") {
			u.Path = "/" + u.Path
		}
	}

	// Add the options to those of the URL, each of which is KEY=VALUE, or a
	// key without a value
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid options of url %q: %v", cmd.URL, err)
	}
	for _, opt := range cmd.Opt {
		key, value, _ := strings.Cut(opt, "=")
		if key = strings.TrimSpace(key); key == "" {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid option: %q", opt)
		}
		query.Add(key, value)
	}

	// The name of a file volume is the URL's host, unless it's set, when the
	// URL has no host, or is the same. Otherwise it's the name option, such
	// as s3://my.bucket/?name=photos, or the host, such as s3://photos/, and
	// --name sets the option, when it isn't the host.
	name := cmd.Name
	if u.Scheme == schema.SchemeFile {
		switch {
		case name == "" && u.Host == "":
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("the volume needs a name, with --name, or as the host of the url: %q", cmd.URL)
		case name == "":
			name = u.Host
		case u.Host == "":
			u.Host = name
		case u.Host != name:
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("the name %q isn't the host of the url: %q", name, cmd.URL)
		}
	} else {
		switch option := query.Get("name"); {
		case name == "" && option == "" && u.Host == "":
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("the volume needs a name, with --name, or the name option, or the host of the url: %q", cmd.URL)
		case name == "" && option == "":
			name = u.Host
		case name == "":
			name = option
		case option == "":
			if name != u.Host {
				query.Set("name", name)
			}
		case option != name:
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("the name %q isn't the name option of the url: %q", name, cmd.URL)
		}
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}

	return schema.Volume{Name: name, URL: u.String()}, nil
}
