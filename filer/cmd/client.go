package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
	term "golang.org/x/term"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	ObjectClientCommands
	SearchClientCommands
	VolumeClientCommands
	MetadataClientCommands
	ArtworkClientCommands
	CredentialClientCommands
	LLMProviderClientCommands
}

type ObjectClientCommands struct {
	ObjectList ObjectListCmd `cmd:"" name:"objects" help:"List server objects." group:"OBJECT"`
}

type SearchClientCommands struct {
	Search SearchCmd `cmd:"" name:"search" help:"Search server objects." group:"SEARCH"`
}

type VolumeClientCommands struct {
	VolumeGet     VolumeGetCmd        `cmd:"" name:"volume" help:"Get a volume by name." group:"VOLUME"`
	VolumeList    VolumeListCmd       `cmd:"" name:"volumes" help:"List server volumes." group:"VOLUME"`
	VolumeCreate  VolumeCreateFileCmd `cmd:"" name:"volume-create-file" help:"Create a new file-backed volume." group:"VOLUME"`
	VolumeMount   VolumeMountCmd      `cmd:"" name:"volume-mount" help:"Mount a volume by name." group:"VOLUME"`
	VolumeUnmount VolumeUnmountCmd    `cmd:"" name:"volume-unmount" help:"Unmount a volume by name." group:"VOLUME"`
	VolumeUpdate  VolumeUpdateCmd     `cmd:"" name:"volume-update" help:"Update a volume by name." group:"VOLUME"`
	VolumeDelete  VolumeDeleteCmd     `cmd:"" name:"volume-delete" help:"Delete a volume by name." group:"VOLUME"`
}

type MetadataClientCommands struct {
	Metadata MetadataCmd `cmd:"" name:"metadata" help:"Extract metadata for a file using the server endpoint." group:"METADATA"`
}

type ArtworkClientCommands struct {
	ArtworkCreate ArtworkCreateCmd `cmd:"" name:"artwork-upload" help:"Upload a new artwork." group:"ARTWORK"`
}

type CredentialClientCommands struct {
	CredentialList   CredentialListCmd   `cmd:"" name:"credentials" help:"List credential keys." group:"CREDENTIAL"`
	CredentialGet    CredentialGetCmd    `cmd:"" name:"credential-get" help:"Get a credential by key." group:"CREDENTIAL"`
	CredentialCreate CredentialCreateCmd `cmd:"" name:"credential-set" help:"Create or update a credential from stdin." group:"CREDENTIAL"`
	CredentialDelete CredentialDeleteCmd `cmd:"" name:"credential-delete" help:"Delete a credential by key." group:"CREDENTIAL"`
}

type LLMProviderClientCommands struct {
	LLMProviderCreate LLMProviderCreateCmd `cmd:"" name:"llm-create" help:"Create or update an LLM provider." group:"LLM PROVIDER"`
}

type ObjectListCmd struct {
	schema.ObjectListRequest
}

type SearchCmd struct {
	schema.SearchListRequest
}

type MetadataCmd struct {
	Path string `arg:"" name:"path" type:"file" help:"Path to the local file."`
}

type CredentialCreateCmd struct {
	Json bool   `name:"json" help:"Interpret the credential value as JSON." negatable:""`
	Key  string `arg:"" name:"key" help:"Credential key (identifier)."`
}

type CredentialGetCmd struct {
	Json bool   `name:"json" help:"Output string credentials as JSON strings." negatable:""`
	Key  string `arg:"" name:"key" help:"Credential key (identifier)."`
}

type CredentialListCmd struct {
	schema.CredentialListRequest
}

type CredentialDeleteCmd struct {
	Key string `arg:"" name:"key" help:"Credential key (identifier)."`
}

type LLMProviderCreateCmd struct {
	schema.LLMProviderCreate
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
		summary := tui.TableSummary("objects", uint(objects.Count), uint(len(objects.Body)), objects.Offset, objects.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// SEARCH COMMANDS

func (cmd *SearchCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "search", func(ctx context.Context, client *httpclient.Client) error {
		results, err := client.Search(ctx, cmd.SearchListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(results)
			return nil
		}

		// Search results list table
		table := tui.TableFor[*schema.SearchResult](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, results.Body...); err != nil {
			return err
		}

		// Search results list summary
		summary := tui.TableSummary("search results", uint(results.Count), uint(len(results.Body)), results.Offset, results.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// VOLUME COMMANDS

type VolumeGetCmd struct {
	Name string `arg:"" name:"name" help:"Volume name."`
}

type VolumeListCmd struct {
	schema.VolumeListRequest
}

type VolumeCreateFileCmd struct {
	Name string `arg:"" name:"name" help:"Volume name."`
	Path string `arg:"" name:"path" type:"file" help:"Path to filesystem."`
}

type VolumeMountCmd struct {
	VolumeGetCmd
}

type VolumeUnmountCmd struct {
	VolumeGetCmd
}

type VolumeUpdateCmd struct {
	VolumeGetCmd
	schema.VolumeMeta
}

type VolumeDeleteCmd struct {
	VolumeGetCmd
}

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
		summary := tui.TableSummary("volumes", uint(volumes.Count), uint(len(volumes.Body)), volumes.Offset, volumes.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *VolumeCreateFileCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume-create-file", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.CreateVolume(ctx, schema.VolumeCreate{
			URL: "file://" + cmd.Name + types.NormalisePath(cmd.Path),
			VolumeMeta: schema.VolumeMeta{
				Enabled: types.Ptr(true),
			},
		})
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeGetCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.GetVolume(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeUpdateCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume-update", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.UpdateVolume(ctx, cmd.Name, cmd.VolumeMeta)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeDeleteCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume-delete", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.DeleteVolume(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(volume)
		return nil
	})
}

func (cmd *VolumeMountCmd) Run(ctx server.Cmd) error {
	cmd2 := VolumeUpdateCmd{
		VolumeGetCmd: VolumeGetCmd{
			Name: cmd.Name,
		},
		VolumeMeta: schema.VolumeMeta{
			Enabled: types.Ptr(true),
		},
	}
	return cmd2.Run(ctx)
}

func (cmd *VolumeUnmountCmd) Run(ctx server.Cmd) error {
	cmd2 := VolumeUpdateCmd{
		VolumeGetCmd: VolumeGetCmd{
			Name: cmd.Name,
		},
		VolumeMeta: schema.VolumeMeta{
			Enabled: types.Ptr(false),
		},
	}
	return cmd2.Run(ctx)
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

///////////////////////////////////////////////////////////////////////////////
// ARTWORK COMMANDS

type ArtworkCreateCmd struct {
	Path string `cmd:"" name:"path" help:"Path to the artwork file." arg:"" required:""`
}

func (cmd *ArtworkCreateCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "artwork-upload", func(ctx context.Context, client *httpclient.Client) error {
		f, err := os.Open(cmd.Path)
		if err != nil {
			return err
		}
		defer f.Close()

		artwork, err := client.CreateArtwork(ctx, f)
		if err != nil {
			return err
		}

		fmt.Println(artwork)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// CREDENTIAL COMMANDS

func (cmd *CredentialListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "credentials", func(ctx context.Context, client *httpclient.Client) error {
		credentials, err := client.ListCredentials(ctx, cmd.CredentialListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(credentials)
			return nil
		}

		// Credentials list table
		table := tui.TableFor[*schema.Credential](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, credentials.Body...); err != nil {
			return err
		}

		// Credentials list summary
		summary := tui.TableSummary("credentials", uint(credentials.Count), uint(len(credentials.Body)), credentials.Offset, credentials.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *CredentialCreateCmd) Run(ctx server.Cmd) error {
	credentials, err := readCredential(term.IsTerminal(int(os.Stdin.Fd())), "Credential: ")
	if err != nil {
		return err
	}
	credentials = bytes.TrimSpace(credentials)
	if len(credentials) == 0 {
		return fmt.Errorf("credential value is required")
	}

	var credentialValue any = string(credentials)
	if cmd.Json {
		if err := json.Unmarshal(credentials, &credentialValue); err != nil {
			return err
		}
	}

	// Perform the request
	return withClient(ctx, "credential-set", func(ctx context.Context, client *httpclient.Client) error {
		credential, err := client.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{
				Key: cmd.Key,
			},
			Credentials: credentialValue,
		})
		if err != nil {
			return err
		}

		fmt.Println(credential)
		return nil
	})
}

func (cmd *CredentialGetCmd) Run(ctx server.Cmd) error {
	passphrase, err := readCredential(term.IsTerminal(int(os.Stdin.Fd())), "Passphrase: ")
	if err != nil {
		return err
	}
	passphrase = bytes.TrimSpace(passphrase)
	if len(passphrase) == 0 {
		return fmt.Errorf("passphrase is required")
	}

	// Perform the request
	return withClient(ctx, "credential-get", func(ctx context.Context, client *httpclient.Client) error {
		var credential any
		if err := client.GetCredential(ctx, schema.CredentialKey{Key: cmd.Key}, string(passphrase), &credential); err != nil {
			return err
		}

		return writeCredential(os.Stdout, credential, cmd.Json)
	})
}

func (cmd *CredentialDeleteCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "credential-delete", func(ctx context.Context, client *httpclient.Client) error {
		credential, err := client.DeleteCredential(ctx, schema.CredentialKey{Key: cmd.Key})
		if err != nil {
			return err
		}

		fmt.Println(credential)
		return nil
	})
}

func readCredential(isTerm bool, prompt string) ([]byte, error) {
	if isTerm {
		fmt.Fprint(os.Stderr, prompt)
		value, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		return value, err
	} else {
		return io.ReadAll(os.Stdin)
	}
}

func writeCredential(w io.Writer, credential any, jsonString bool) error {
	if value, ok := credential.(string); ok && !jsonString {
		_, err := fmt.Fprintln(w, value)
		return err
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(credential)
}

///////////////////////////////////////////////////////////////////////////////
// LLM PROVIDER COMMANDS

func (cmd *LLMProviderCreateCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "llmprovider-create", func(ctx context.Context, client *httpclient.Client) error {
		provider, err := client.CreateLLMProvider(ctx, cmd.LLMProviderCreate)
		if err != nil {
			return err
		}

		fmt.Println(provider)
		return nil
	})
}
