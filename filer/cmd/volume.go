package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	changesclient "github.com/mutablelogic/go-server/broadcaster/httpclient"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeCommands struct {
	VolumeList     VolumeListCmd     `cmd:"" name:"volumes" help:"List volumes, with the URLs of their backends." group:"VOLUME"`
	VolumeGet      VolumeGetCmd      `cmd:"" name:"volume" help:"Get a volume by its name, with the URL of its backend." group:"VOLUME"`
	VolumeSchemes  VolumeSchemesCmd  `cmd:"" name:"volume-schemes" help:"List the schemes of the URLs which volumes can be created from." group:"VOLUME"`
	VolumeCreate   VolumeCreateCmd   `cmd:"" name:"volume-create" help:"Create a volume from the URL of its backend, or a directory." group:"VOLUME"`
	VolumeCreateFS VolumeCreateFSCmd `cmd:"" name:"volume-create-fs" help:"Create a volume of a directory on the server." group:"VOLUME"`
	VolumeCreateS3 VolumeCreateS3Cmd `cmd:"" name:"volume-create-s3" help:"Create a volume of an S3 bucket, or one which is compatible, such as MinIO's or SeaweedFS's." group:"VOLUME"`
	VolumeUpdate   VolumeUpdateCmd   `cmd:"" name:"volume-update" help:"Change the options of a volume by its name." group:"VOLUME"`
	VolumeDelete   VolumeDeleteCmd   `cmd:"" name:"volume-delete" help:"Delete a volume by its name, without deleting its files." group:"VOLUME"`
	VolumeWatch    VolumeWatchCmd    `cmd:"" name:"volume-watch" help:"Show the changes to the objects of volumes, as they happen, until it's interrupted." group:"VOLUME"`
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

type VolumeCreateFSCmd struct {
	Name     string   `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Path     string   `arg:"" name:"path" help:"Absolute path of the directory, on the server, such as /mnt/media."`
	Readonly bool     `name:"readonly" help:"Files can only be read, and not created or deleted."`
	Skip     []string `name:"skip" sep:"none" placeholder:"REGEXP" help:"Regular expression of the names of files and directories which aren't listed, such as '^\\.', which can be repeated, and replaces the defaults. An empty one, --skip '', lists every name."`
	Package  []string `name:"package" sep:"none" placeholder:"REGEXP" help:"Regular expression of the names of directories which are packages, listed as files, such as '\\.bundle$', which can be repeated, and replaces the defaults. An empty one, --package '', has no packages."`
}

type VolumeCreateS3Cmd struct {
	Name         string `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Bucket       string `arg:"" name:"bucket" help:"Bucket, and optionally a prefix of its keys, such as my.bucket or my.bucket/photos."`
	Region       string `name:"region" help:"Region of the bucket, which is its own, when it's not set."`
	Endpoint     string `name:"endpoint" placeholder:"URL" help:"URL of a service which is compatible with S3, such as http://localhost:9000 for MinIO."`
	Profile      string `name:"profile" help:"Profile of the server's shared AWS configuration, whose credentials are used."`
	AccessKey    string `name:"access-key-credential" placeholder:"NAME" help:"Name of the server's credential of the access key ID, such as S3_KEY, of its environment, which it looks up, so the key isn't sent, or stored."`
	SecretKey    string `name:"secret-key-credential" placeholder:"NAME" help:"Name of the server's credential of the secret access key, such as S3_SECRET, which it looks up, so the secret isn't sent, or stored."`
	SessionToken string `name:"session-token-credential" placeholder:"NAME" help:"Name of the server's credential of the session token, such as S3_TOKEN, which it looks up."`
	Anonymous    bool   `name:"anonymous" help:"Requests aren't signed, so they're without credentials, and the volume is read-only."`
	Readonly     bool   `name:"readonly" help:"Objects can only be read, and not created or deleted."`
	SeaweedFS    string `name:"seaweedfs-filer-grpc" placeholder:"HOST:PORT" help:"gRPC service of the SeaweedFS filer whose S3 gateway the endpoint is, such as 192.168.86.21:18888, so the bucket's objects are watched."`
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
	return createVolume(ctx, "filer.VolumeCreateCmd", volume)
}

func (cmd *VolumeCreateFSCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	return createVolume(ctx, "filer.VolumeCreateFSCmd", volume)
}

func (cmd *VolumeCreateS3Cmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	return createVolume(ctx, "filer.VolumeCreateS3Cmd", volume)
}

// createVolume creates the volume, and prints it
func createVolume(ctx server.Cmd, span string, volume schema.Volume) error {
	return withClient(ctx, span, func(ctx context.Context, client *httpclient.Client) error {
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

// Run watches the changes to the objects of the volumes, or of every volume,
// with go-server's /changes, of the filer schema
func (cmd *VolumeWatchCmd) Run(ctx server.Cmd) error {
	debug := ctx.IsDebug()
	filter := changes.ChangeFilter{Schema: schema.ChangeSchema}
	if len(cmd.Name) == 1 {
		filter.Table = cmd.Name[0]
	}
	return withChangesClient(ctx, "filer.VolumeWatchCmd", func(ctx context.Context, client *changesclient.Client) error {
		return client.WatchChanges(ctx, filter, func(change changes.Change) error {
			event, ok := schema.ChangeEvent(change)
			if !ok || (len(cmd.Name) > 1 && !slices.Contains(cmd.Name, event.Name)) {
				return nil
			}
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
	u, err := gofiler.ParseURL(cmd.URL)
	if err != nil {
		return schema.Volume{}, err
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

// volume returns the volume of a directory on the server, whose URL is
// file://<name>/<path>, with its options. The path is the server's, so it
// must be absolute, as it can't be made absolute here: /mnt/media, or, on
// Windows, C:\media, which is /C:/media.
func (cmd *VolumeCreateFSCmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	// A path is used as it is, as a name can have spaces, or a backslash,
	// on Unix, except a path on Windows, with a drive, whose backslashes are
	// separators
	path := cmd.Path
	switch {
	case strings.HasPrefix(path, "/"):
	case windowsDrive.MatchString(path):
		path = "/" + strings.ReplaceAll(path, "\\", "/")
	default:
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("the path of the directory, on the server, must be absolute: %q", cmd.Path)
	}
	query := url.Values{}
	if cmd.Readonly {
		query.Set("readonly", "true")
	}
	for _, skip := range cmd.Skip {
		query.Add("skip", skip)
	}
	for _, pkg := range cmd.Package {
		query.Add("package", pkg)
	}
	u := url.URL{Scheme: schema.SchemeFile, Host: cmd.Name, Path: path, RawQuery: query.Encode()}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

// volume returns the volume of a bucket, whose URL is
// s3://<bucket>/<prefix>, with its options, as they are, and the name
// option, when the volume's name isn't the bucket's. Its credentials are the
// names of the server's, which it looks up, as placeholders, such as
// ${S3_SECRET}, so they aren't sent, or stored.
func (cmd *VolumeCreateS3Cmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(cmd.Bucket, "/"), "/")
	if bucket == "" {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid bucket: %q", cmd.Bucket)
	}
	prefix = strings.TrimSuffix(prefix, "/")
	query := url.Values{}
	if cmd.Name != bucket {
		query.Set("name", cmd.Name)
	}
	for key, value := range map[string]string{
		"region":               cmd.Region,
		"endpoint":             cmd.Endpoint,
		"profile":              cmd.Profile,
		"seaweedfs-filer-grpc": cmd.SeaweedFS,
	} {
		if value != "" {
			query.Set(key, value)
		}
	}
	for key, name := range map[string]string{
		"access-key-id":     cmd.AccessKey,
		"secret-access-key": cmd.SecretKey,
		"session-token":     cmd.SessionToken,
	} {
		if name == "" {
			continue
		} else if !credentialName.MatchString(name) {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid name of a credential: %q, which is letters, digits and underscores, such as S3_SECRET", name)
		}
		query.Set(key, "${"+name+"}")
	}
	if cmd.Anonymous {
		query.Set("anonymous", "true")
	}
	if cmd.Readonly {
		query.Set("readonly", "true")
	}
	u := url.URL{Scheme: schema.SchemeS3, Host: bucket, RawQuery: query.Encode()}
	if prefix != "" {
		u.Path = "/" + prefix
	}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

// windowsDrive is a path of a directory on Windows, with a drive, such as
// C:\media or C:/media, and credentialName is the name of a credential, as
// an environment variable's, such as S3_SECRET
var (
	windowsDrive   = regexp.MustCompile(`^[A-Za-z]:[\\/]`)
	credentialName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)
