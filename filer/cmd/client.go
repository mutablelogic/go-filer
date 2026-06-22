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
	term "golang.org/x/term"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	ObjectClientCommands
	VolumeClientCommands
	MetadataClientCommands
	CredentialClientCommands
	LLMProviderClientCommands
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

type VolumeCreateCmd struct {
	schema.VolumeCreate
}

type VolumeListCmd struct {
	schema.VolumeListRequest
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
		summary := tui.TableSummary("credentials", uint(credentials.Count), credentials.Offset, credentials.Limit)
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
