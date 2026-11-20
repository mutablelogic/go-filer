package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	// Packages
	credentialclient "github.com/mutablelogic/go-auth/credential/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	rss "github.com/mutablelogic/go-filer/backend/rss"
	registry "github.com/mutablelogic/go-filer/registry"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	server "github.com/mutablelogic/go-server"
	changesclient "github.com/mutablelogic/go-server/broadcaster/httpclient"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeCommands struct {
	VolumeList            VolumeListCmd            `cmd:"" name:"volumes" help:"List volumes, with the URLs of their backends." group:"VOLUME"`
	VolumeGet             VolumeGetCmd             `cmd:"" name:"volume" help:"Get a volume by its name, with the URL of its backend." group:"VOLUME"`
	VolumeBackends        VolumeBackendsCmd        `cmd:"" name:"volume-backends" aliases:"volume-schemes" help:"List the backends the server runs, of their schemes, which volumes can be created of, with what each is." group:"VOLUME"`
	VolumeCreate          VolumeCreateCmd          `cmd:"" name:"volume-create" help:"Create a volume from the URL of its backend, or a directory." group:"VOLUME"`
	VolumeCreateFS        VolumeCreateFSCmd        `cmd:"" name:"volume-create-fs" help:"Create a volume of a directory on the server." group:"VOLUME"`
	VolumeCreateS3        VolumeCreateS3Cmd        `cmd:"" name:"volume-create-s3" help:"Create a volume of an S3 bucket, or one which is compatible, such as MinIO's or SeaweedFS's." group:"VOLUME"`
	VolumeCreateGTasks    VolumeCreateGTasksCmd    `cmd:"" name:"volume-create-gtasks" help:"Create a volume of a Google account's tasks, signing in with Google, in the browser, when the server needs it." group:"VOLUME"`
	VolumeCreateGContacts VolumeCreateGContactsCmd `cmd:"" name:"volume-create-gcontacts" help:"Create a volume of a Google account's contacts, as vCards, signing in with Google, in the browser, when the server needs it." group:"VOLUME"`
	VolumeCreateGCalendar VolumeCreateGCalendarCmd `cmd:"" name:"volume-create-gcalendar" help:"Create a volume of a Google account's calendars' events, as iCalendars, of the days before today and after it, signing in with Google, in the browser, when the server needs it." group:"VOLUME"`
	VolumeCreateRSS       VolumeCreateRSSCmd       `cmd:"" name:"volume-create-rss" help:"Create a volume of an RSS feed's items, read-only, of its URL." group:"VOLUME"`
	VolumeUpdate          VolumeUpdateCmd          `cmd:"" name:"volume-update" help:"Change the options of a volume by its name, and whether it's enabled, or open it again, without any, signing in when its user needs to, such as when it's offline." group:"VOLUME"`
	VolumeMount           VolumeMountCmd           `cmd:"" name:"volume-mount" help:"Enable a volume by its name, so the server mounts it, and does when it starts, signing in when its user needs to." group:"VOLUME"`
	VolumeUnmount         VolumeUnmountCmd         `cmd:"" name:"volume-unmount" help:"Disable a volume by its name, so the server unmounts it, keeping it, and its index." group:"VOLUME"`
	VolumeDelete          VolumeDeleteCmd          `cmd:"" name:"volume-delete" help:"Delete a volume by its name, without deleting its files." group:"VOLUME"`
	VolumeWatch           VolumeWatchCmd           `cmd:"" name:"volume-watch" help:"Show the changes to the objects of volumes, as they happen, until it's interrupted." group:"VOLUME"`
}

type VolumeListCmd struct{}

type VolumeBackendsCmd struct{}

type VolumeGetCmd struct {
	Name string `arg:"" name:"name" help:"Volume name."`
}

type VolumeUpdateCmd struct {
	VolumeGetCmd
	NoBrowser   bool     `name:"no-browser" help:"Print the URL to sign in at, when its user needs to sign in, rather than opening it."`
	Opt         []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Option to set, such as readonly=true, which can be repeated, when the option has each of the values, such as -o skip=^\\. -o skip=\\.tmp$. An option without a value, such as skip, has none."`
	Unset       []string `name:"unset" sep:"none" placeholder:"KEY" help:"Option to remove, so it's the default, which can be repeated."`
	Enabled     *bool    `name:"enabled" negatable:"" help:"Enable the volume, so it's mounted, or disable it, with --no-enabled, so it's unmounted, which is kept as it is without either."`
	Description *string  `name:"description" help:"What the volume is, such as \"Family photos\", which is a shortcut of -o description=..., or the backend's, when it's empty."`
}

type VolumeMountCmd struct {
	VolumeGetCmd
	NoBrowser bool `name:"no-browser" help:"Print the URL to sign in at, when its user needs to sign in, rather than opening it."`
}

type VolumeUnmountCmd struct {
	VolumeGetCmd
}

type VolumeWatchCmd struct {
	Name []string `arg:"" name:"name" optional:"" help:"Volumes to watch, which is every volume without any."`
}

type VolumeDeleteCmd struct {
	VolumeGetCmd
}

type VolumeCreateCmd struct {
	URL         string   `arg:"" name:"url" help:"URL of the volume's backend, such as file://media/mnt/folder, whose host is the volume's name, or a directory on the server, such as /mnt/media, with --name, or a bucket, such as s3://bucket/prefix?name=photos."`
	Name        string   `name:"name" help:"Volume name, which is an identifier. For a file URL it's the host, and otherwise the name option, or the host, which it defaults to, and sets the option when it isn't the host."`
	Opt         []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Option of the volume, such as readonly=true, skip=^\\. or package=\\.bundle$, which can be repeated, and is added to those of the URL. An option without a value, such as readonly, is empty, which for readonly is true."`
	Description string   `name:"description" help:"What the volume is, such as \"Family photos\", which the server sets, of its backend, when it isn't set."`
}

type VolumeCreateFSCmd struct {
	Name        string   `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Path        string   `arg:"" name:"path" help:"Absolute path of the directory, on the server, such as /mnt/media."`
	Readonly    bool     `name:"readonly" help:"Files can only be read, and not created or deleted."`
	Skip        []string `name:"skip" sep:"none" placeholder:"REGEXP" help:"Regular expression of the names of files and directories which aren't listed, such as '^\\.', which can be repeated, and replaces the defaults. An empty one, --skip '', lists every name."`
	Package     []string `name:"package" sep:"none" placeholder:"REGEXP" help:"Regular expression of the names of directories which are packages, listed as files, such as '\\.bundle$', which can be repeated, and replaces the defaults. An empty one, --package '', has no packages."`
	Opt         []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=folders, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description string   `name:"description" help:"What the volume is, such as \"Family photos\", which the server sets, of its backend, when it isn't set."`
}

type VolumeCreateS3Cmd struct {
	Name         string   `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Bucket       string   `arg:"" name:"bucket" help:"Bucket, and optionally a prefix of its keys, such as my.bucket or my.bucket/photos."`
	Region       string   `name:"region" help:"Region of the bucket, which is its own, when it's not set."`
	Endpoint     string   `name:"endpoint" placeholder:"URL" help:"URL of a service which is compatible with S3, such as http://localhost:9000 for MinIO."`
	Profile      string   `name:"profile" help:"Profile of the server's shared AWS configuration, whose credentials are used."`
	AccessKey    string   `name:"access-key-credential" placeholder:"NAME" help:"Name of the server's credential of the access key ID, such as S3_KEY, of its environment, which it looks up, so the key isn't sent, or stored."`
	SecretKey    string   `name:"secret-key-credential" placeholder:"NAME" help:"Name of the server's credential of the secret access key, such as S3_SECRET, which it looks up, so the secret isn't sent, or stored."`
	SessionToken string   `name:"session-token-credential" placeholder:"NAME" help:"Name of the server's credential of the session token, such as S3_TOKEN, which it looks up."`
	Anonymous    bool     `name:"anonymous" help:"Requests aren't signed, so they're without credentials, and the volume is read-only."`
	Readonly     bool     `name:"readonly" help:"Objects can only be read, and not created or deleted."`
	SeaweedFS    string   `name:"seaweedfs-filer-grpc" placeholder:"HOST:PORT" help:"gRPC service of the SeaweedFS filer whose S3 gateway the endpoint is, such as 192.168.86.21:18888, so the bucket's objects are watched."`
	Opt          []string `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=folders, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description  string   `name:"description" help:"What the volume is, such as \"Family photos\", which the server sets, of its backend, when it isn't set."`
}

type VolumeCreateGContactsCmd struct {
	Name            string         `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Credential      string         `name:"credential" placeholder:"KEY" help:"Key of the server's credential of the sign-in, which is GOOGLE_TOKEN, unless it's set, such as GOOGLE_WORK, for another account."`
	Quota           *uint          `name:"quota" help:"Most requests to Google each minute, or 0 not to limit them, which is 120, unless it's set."`
	RefreshInterval *time.Duration `name:"refresh-interval" placeholder:"DURATION" help:"How often changes are polled for, or 0 not to watch them, which is a minute, unless it's set."`
	NoBrowser       bool           `name:"no-browser" help:"Print the URL to sign in with Google at, rather than opening it in the browser."`
	Opt             []string       `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=folders, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description     string         `name:"description" help:"What the volume is, such as \"Family photos\", which the server sets, of its backend, when it isn't set."`
}

type VolumeCreateGCalendarCmd struct {
	Name            string         `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Writable        bool           `name:"writable" help:"Events can be created, updated and deleted, of the event action, which needs Google's calendar scope, rather than calendar.readonly."`
	Window          string         `name:"window" placeholder:"DURATION" help:"The time of events before today, and after it, such as 30d, 52w or 720h, which is 365d, unless it's set."`
	Credential      string         `name:"credential" placeholder:"KEY" help:"Key of the server's credential of the sign-in, which is GOOGLE_TOKEN, unless it's set, such as GOOGLE_WORK, for another account."`
	Quota           *uint          `name:"quota" help:"Most requests to Google each minute, or 0 not to limit them, which is 120, unless it's set."`
	RefreshInterval *time.Duration `name:"refresh-interval" placeholder:"DURATION" help:"How often changes are polled for, or 0 not to watch them, which is a minute, unless it's set."`
	NoBrowser       bool           `name:"no-browser" help:"Print the URL to sign in with Google at, rather than opening it in the browser."`
	Opt             []string       `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=metadata, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description     string         `name:"description" help:"What the volume is, such as \"Work calendar\", which the server sets, of its backend, when it isn't set."`
}

type VolumeCreateRSSCmd struct {
	Name            string         `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Feed            string         `arg:"" name:"url" help:"URL of the feed, which is http or https."`
	RefreshInterval *time.Duration `name:"refresh-interval" placeholder:"DURATION" help:"How long a feed which is fetched is kept, which is 5m, unless it's set."`
	Opt             []string       `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=metadata, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description     string         `name:"description" help:"What the volume is, such as \"News\", which the server sets, of the feed's title, when it isn't set."`
}

type VolumeCreateGTasksCmd struct {
	Name            string         `arg:"" name:"name" help:"Volume name, which is an identifier."`
	Writable        bool           `name:"writable" help:"Tasks can be created, updated and deleted, which needs Google's tasks scope, rather than tasks.readonly."`
	Credential      string         `name:"credential" placeholder:"KEY" help:"Key of the server's credential of the sign-in, which is GOOGLE_TOKEN, unless it's set, such as GOOGLE_WORK, for another account."`
	Quota           *uint          `name:"quota" help:"Most requests to Google each minute, or 0 not to limit them, which is 120, unless it's set."`
	RefreshInterval *time.Duration `name:"refresh-interval" placeholder:"DURATION" help:"How often changes are polled for, or 0 not to watch them, which is a minute, unless it's set."`
	Hidden          bool           `name:"hidden" help:"Include tasks which are hidden, as they're completed, and cleared from their list, which aren't, unless it's set."`
	NoBrowser       bool           `name:"no-browser" help:"Print the URL to sign in with Google at, rather than opening it in the browser."`
	Opt             []string       `name:"opt" sep:"none" short:"o" placeholder:"KEY=VALUE" help:"Another option of the volume, such as index=folders, which can be repeated, and replaces one which a flag sets. An option without a value is empty."`
	Description     string         `name:"description" help:"What the volume is, such as \"Family photos\", which the server sets, of its backend, when it isn't set."`
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

func (cmd *VolumeBackendsCmd) Run(ctx server.Cmd) error {
	width := ctx.IsTerm()
	return withClient(ctx, "filer.VolumeBackendsCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListVolumes(ctx)
		if err != nil {
			return err
		}

		// The backends, of their schemes, and what each is
		table := tui.TableFor[schema.Backend](tui.SetWidth(width))
		_, err = table.Write(os.Stdout, list.Backends...)
		return err
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

// Run creates the volume of Google Tasks, and, when the server says its
// user needs to sign in, signs in with Google, in the browser, and creates
// it again
func (cmd *VolumeCreateGTasksCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	// The client which sends the sign-in's code, which isn't traced to
	// stderr, with --debug or --verbose, as its body is a secret
	signInClient, err := newSignInClient(ctx)
	if err != nil {
		return err
	}
	return createSignedIn(ctx, "filer.VolumeCreateGTasksCmd", volume, signInClient, cmd.NoBrowser)
}

// Run creates a volume of contacts, as a volume of tasks is created
func (cmd *VolumeCreateGCalendarCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	signInClient, err := newSignInClient(ctx)
	if err != nil {
		return err
	}
	return createSignedIn(ctx, "filer.VolumeCreateGCalendarCmd", volume, signInClient, cmd.NoBrowser)
}

func (cmd *VolumeCreateRSSCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	return createVolume(ctx, "filer.VolumeCreateRSSCmd", volume)
}

func (cmd *VolumeCreateGContactsCmd) Run(ctx server.Cmd) error {
	volume, err := cmd.volume()
	if err != nil {
		return err
	}
	signInClient, err := newSignInClient(ctx)
	if err != nil {
		return err
	}
	return createSignedIn(ctx, "filer.VolumeCreateGContactsCmd", volume, signInClient, cmd.NoBrowser)
}

// createSignedIn creates a volume, and, when its user needs to sign in,
// signs in with Google, in the browser, and creates it again, and prints it
func createSignedIn(ctx server.Cmd, span string, volume schema.Volume, signInClient *credentialclient.Client, noBrowser bool) error {
	return withClient(ctx, span, func(ctx context.Context, client *httpclient.Client) error {
		if err := checkBackend(ctx, client, volume.URL); err != nil {
			return err
		}
		req := schema.VolumeCreateRequest{Name: volume.Name, URL: volume.URL}
		created, err := client.CreateVolume(ctx, req)
		if signIn := httpclient.SignInOf(err); signIn != nil {
			if err := signInWithBrowser(ctx, signInClient, *signIn, noBrowser); err != nil {
				return err
			}
			created, err = client.CreateVolume(ctx, req)
		}
		if err != nil {
			return err
		}
		fmt.Println(created)
		return nil
	})
}

// createVolume creates the volume, and prints it
func createVolume(ctx server.Cmd, span string, volume schema.Volume) error {
	return withClient(ctx, span, func(ctx context.Context, client *httpclient.Client) error {
		if err := checkBackend(ctx, client, volume.URL); err != nil {
			return err
		}
		volume, err := client.CreateVolume(ctx, schema.VolumeCreateRequest{Name: volume.Name, URL: volume.URL})
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
	return updateVolume(ctx, "filer.VolumeUpdateCmd", cmd.Name, patch, cmd.Enabled, cmd.NoBrowser)
}

// Run enables the volume, so it's mounted
func (cmd *VolumeMountCmd) Run(ctx server.Cmd) error {
	return updateVolume(ctx, "filer.VolumeMountCmd", cmd.Name, nil, types.Ptr(true), cmd.NoBrowser)
}

// Run disables the volume, so it's unmounted
func (cmd *VolumeUnmountCmd) Run(ctx server.Cmd) error {
	return updateVolume(ctx, "filer.VolumeUnmountCmd", cmd.Name, nil, types.Ptr(false), true)
}

// updateVolume updates the options of a volume, and whether it's enabled,
// when it isn't nil, signing in when its user needs to, and prints it
func updateVolume(ctx server.Cmd, span, name string, patch schema.VolumePatch, enabled *bool, noBrowser bool) error {
	signInClient, err := newSignInClient(ctx)
	if err != nil {
		return err
	}
	return withClient(ctx, span, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.UpdateVolume(ctx, name, patch, enabled)
		if signIn := httpclient.SignInOf(err); signIn != nil {
			if err := signInWithBrowser(ctx, signInClient, *signIn, noBrowser); err != nil {
				return err
			}
			volume, err = client.UpdateVolume(ctx, name, patch, enabled)
		}
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
	if cmd.Description != nil {
		if _, exists := values[schema.OptDescription]; exists {
			return nil, gofiler.ErrBadParameter.With("the description is set with --description, and an option")
		}
		values[schema.OptDescription] = []string{*cmd.Description}
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
	return patch, nil
}

// volume returns the volume to create. The name is the URL's host, unless
// it's set, when a URL without a host has the name as its host, so
// file:///mnt/media is file://<name>/mnt/media, and a URL
// without a scheme is a directory, with the options after a "?", such as
// /mnt/media?readonly=true, which is made absolute, as the server's
// directory, so it's best given as one which is absolute.
func (cmd *VolumeCreateCmd) volume() (schema.Volume, error) {
	u, err := registry.ParseURL(cmd.URL)
	if err != nil {
		return schema.Volume{}, err
	}

	// Add the options to those of the URL, each of which is KEY=VALUE, or a
	// key without a value
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid options of url %q: %v", cmd.URL, err)
	}
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	opts, err := parseOpts(cmd.Opt)
	if err != nil {
		return schema.Volume{}, err
	}
	for key, values := range opts {
		if key == schema.OptDescription {
			query.Del(key)
		}
		query[key] = append(query[key], values...)
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
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
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
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
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

// volume returns the volume of Google Tasks, whose URL is gtasks://<name>,
// with its parameters, of the flags which are set
func (cmd *VolumeCreateGContactsCmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	query := url.Values{}
	if cmd.Credential != "" {
		if !credentialName.MatchString(cmd.Credential) {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid key of a credential: %q", cmd.Credential)
		}
		query.Set("credential", cmd.Credential)
	}
	if cmd.Quota != nil {
		query.Set("quota", strconv.FormatUint(uint64(*cmd.Quota), 10))
	}
	if cmd.RefreshInterval != nil {
		query.Set("refresh_interval", cmd.RefreshInterval.String())
	}
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
	}
	u := url.URL{Scheme: google.ContactsScheme, Host: cmd.Name, RawQuery: query.Encode()}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

// volume returns the volume of Google Calendar, whose URL is
// gcalendar://<name>, with its parameters, of the flags which are set
func (cmd *VolumeCreateGCalendarCmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	query := url.Values{}
	if cmd.Writable {
		query.Set("readonly", "false")
	}
	if cmd.Window != "" {
		query.Set("window", cmd.Window)
	}
	if cmd.Credential != "" {
		if !credentialName.MatchString(cmd.Credential) {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid key of a credential: %q", cmd.Credential)
		}
		query.Set("credential", cmd.Credential)
	}
	if cmd.Quota != nil {
		query.Set("quota", strconv.FormatUint(uint64(*cmd.Quota), 10))
	}
	if cmd.RefreshInterval != nil {
		query.Set("refresh_interval", cmd.RefreshInterval.String())
	}
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
	}
	u := url.URL{Scheme: google.CalendarScheme, Host: cmd.Name, RawQuery: query.Encode()}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

func (cmd *VolumeCreateRSSCmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	feed, err := url.Parse(cmd.Feed)
	if err != nil || (feed.Scheme != "http" && feed.Scheme != "https") || feed.Host == "" {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("the url of a feed is http or https: %q", cmd.Feed)
	}

	// The feed is the URL's path, rss://<name>/<host>/<path>, with its own
	// parameters, and is http, rather than https, with insecure=true
	query := feed.Query()
	if feed.Scheme == "http" {
		query.Set("insecure", "true")
	}
	if cmd.RefreshInterval != nil {
		query.Set("refresh_interval", cmd.RefreshInterval.String())
	}
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
	}
	u := url.URL{Scheme: rss.Scheme, Host: cmd.Name, Path: "/" + feed.Host + feed.Path, RawPath: "/" + feed.Host + feed.EscapedPath(), RawQuery: query.Encode()}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

func (cmd *VolumeCreateGTasksCmd) volume() (schema.Volume, error) {
	if !types.IsIdentifier(cmd.Name) {
		return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid volume name: %q", cmd.Name)
	}
	query := url.Values{}
	if cmd.Writable {
		query.Set("readonly", "false")
	}
	if cmd.Credential != "" {
		if !credentialName.MatchString(cmd.Credential) {
			return schema.Volume{}, gofiler.ErrBadParameter.Withf("invalid key of a credential: %q", cmd.Credential)
		}
		query.Set("credential", cmd.Credential)
	}
	if cmd.Quota != nil {
		query.Set("quota", strconv.FormatUint(uint64(*cmd.Quota), 10))
	}
	if cmd.RefreshInterval != nil {
		query.Set("refresh_interval", cmd.RefreshInterval.String())
	}
	if cmd.Hidden {
		query.Set("hidden", "true")
	}
	if cmd.Description != "" {
		query.Set(schema.OptDescription, cmd.Description)
	}
	if err := setOpts(query, cmd.Opt); err != nil {
		return schema.Volume{}, err
	}
	u := url.URL{Scheme: google.TasksScheme, Host: cmd.Name, RawQuery: query.Encode()}
	return schema.Volume{Name: cmd.Name, URL: u.String()}, nil
}

// parseOpts returns the options of --opt, each of which is KEY=VALUE, or a
// key without a value, which is empty, and can be repeated
func parseOpts(opts []string) (url.Values, error) {
	result := url.Values{}
	for _, opt := range opts {
		key, value, _ := strings.Cut(opt, "=")
		if key = strings.TrimSpace(key); key == "" {
			return nil, gofiler.ErrBadParameter.Withf("invalid option: %q", opt)
		}
		result.Add(key, value)
	}
	return result, nil
}

// setOpts sets the options of --opt, which replace those of a flag
func setOpts(query url.Values, opts []string) error {
	values, err := parseOpts(opts)
	if err != nil {
		return err
	}
	for key, v := range values {
		query[key] = v
	}
	return nil
}

// checkBackend returns ErrBadParameter when the server doesn't run the
// backend of a volume's URL, of its scheme, with those it does run, before
// the volume is created, such as before signing in
func checkBackend(ctx context.Context, client *httpclient.Client, volumeURL string) error {
	u, err := url.Parse(volumeURL)
	if err != nil {
		return gofiler.ErrBadParameter.Withf("invalid url: %v", err)
	}
	list, err := client.ListVolumes(ctx)
	if err != nil {
		return err
	}
	schemes := make([]string, 0, len(list.Backends))
	for _, backend := range list.Backends {
		schemes = append(schemes, backend.Scheme)
	}
	if !slices.Contains(schemes, u.Scheme) {
		return gofiler.ErrBadParameter.Withf("the server doesn't run the %q backend, but %s", u.Scheme, strings.Join(schemes, ", "))
	}
	return nil
}
