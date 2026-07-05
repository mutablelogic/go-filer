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
	httpclient "github.com/mutablelogic/go-filer/credential/httpclient"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	term "golang.org/x/term"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	CredentialList   CredentialListCmd   `cmd:"" name:"credentials" help:"List credential keys." group:"CREDENTIAL"`
	CredentialGet    CredentialGetCmd    `cmd:"" name:"credential-get" help:"Get a credential by key." group:"CREDENTIAL"`
	CredentialCreate CredentialCreateCmd `cmd:"" name:"credential-set" help:"Create or update a credential from stdin." group:"CREDENTIAL"`
	CredentialRotate CredentialRotateCmd `cmd:"" name:"credential-rotate" help:"Rotate a credential by key." group:"CREDENTIAL"`
	CredentialDelete CredentialDeleteCmd `cmd:"" name:"credential-delete" help:"Delete a credential by key." group:"CREDENTIAL"`
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

type CredentialRotateCmd struct {
	Key string `arg:"" name:"key" help:"Credential key (identifier)."`
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

func (cmd *CredentialRotateCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "credential-rotate", func(ctx context.Context, client *httpclient.Client) error {
		credential, err := client.RotateCredential(ctx, schema.CredentialKey{Key: cmd.Key})
		if err != nil {
			return err
		}

		fmt.Println(credential)
		return nil
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
