package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"

	// Packages
	oidc "github.com/mutablelogic/go-auth/auth/oidc"
	webcallback "github.com/mutablelogic/go-auth/auth/webcallback"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	google "github.com/mutablelogic/go-filer/backend/google"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
	"github.com/pkg/browser"
	errgroup "golang.org/x/sync/errgroup"
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
	GoogleClientCommands
}

type ObjectClientCommands struct {
	ObjectList ObjectListCmd `cmd:"" name:"objects" help:"List server objects." group:"OBJECT"`
	ObjectGet  ObjectGetCmd  `cmd:"" name:"object" help:"Get object metadata by volume and path." group:"OBJECT"`
}

type SearchClientCommands struct {
	Search SearchCmd `cmd:"" name:"search" help:"Search server objects." group:"SEARCH"`
}

type VolumeClientCommands struct {
	VolumeGet        VolumeGetCmd        `cmd:"" name:"volume" help:"Get a volume by name." group:"VOLUME"`
	VolumeList       VolumeListCmd       `cmd:"" name:"volumes" help:"List server volumes." group:"VOLUME"`
	VolumeCreateFile VolumeCreateFileCmd `cmd:"" name:"volume-create-file" help:"Create a new file-backed volume." group:"VOLUME"`
	VolumeCreateS3   VolumeCreateS3Cmd   `cmd:"" name:"volume-create-s3" help:"Create a new S3-backed volume." group:"VOLUME"`
	VolumeMount      VolumeMountCmd      `cmd:"" name:"volume-mount" help:"Mount a volume by name." group:"VOLUME"`
	VolumeUnmount    VolumeUnmountCmd    `cmd:"" name:"volume-unmount" help:"Unmount a volume by name." group:"VOLUME"`
	VolumeUpdate     VolumeUpdateCmd     `cmd:"" name:"volume-update" help:"Update a volume by name." group:"VOLUME"`
	VolumeDelete     VolumeDeleteCmd     `cmd:"" name:"volume-delete" help:"Delete a volume by name." group:"VOLUME"`
	VolumeReindex    VolumeReindexCmd    `cmd:"" name:"volume-reindex" help:"Reindex a volume by name." group:"VOLUME"`
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

type GoogleClientCommands struct {
	GoogleLogin GoogleLoginCmd `cmd:"" name:"google-login" help:"Login to the Google." group:"GOOGLE"`
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

func withGoogleClient(ctx server.Cmd, span string, fn func(context.Context, *google.Client) error) error {
	_, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	} else if client, err := google.New(opts...); err != nil {
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

type ObjectListCmd struct {
	schema.ObjectListRequest
}

type ObjectGetCmd struct {
	schema.ObjectKey
}

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

func (cmd *ObjectGetCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "object", func(ctx context.Context, client *httpclient.Client) error {
		object, err := client.GetObject(ctx, cmd.Volume, cmd.Path)
		if err != nil {
			return err
		}

		fmt.Println(object)
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

type VolumeCreateS3Cmd struct {
	URL       *url.URL `arg:"" name:"url" type:"url" help:"S3 URL (s3://bucket-name/prefix)."`
	Endpoint  *url.URL `name:"endpoint" type:"url" help:"Custom S3 endpoint URL."`
	Anonymous bool     `name:"anonymous" help:"Use anonymous credentials for S3." negatable:""`
	AccessKey string   `name:"access-key" help:"AWS access credential."`
	SecretKey string   `name:"secret-key" help:"AWS secret credential."`
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

type VolumeReindexCmd struct {
	VolumeGetCmd
	schema.ObjectListFilters
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

func (cmd *VolumeCreateS3Cmd) Run(ctx server.Cmd) error {
	// Fix the URL to include additional parameters
	if cmd.URL.Scheme != "s3" {
		return fmt.Errorf("invalid URL scheme: %q (expected 's3')", cmd.URL.Scheme)
	}
	q := url.Values{}
	if cmd.Anonymous {
		q.Set("anonymous", "true")
	}
	if cmd.Endpoint != nil {
		q.Set("endpoint", cmd.Endpoint.String())
	}
	if cmd.AccessKey != "" {
		q.Set("access-key", cmd.AccessKey)
	}
	if cmd.SecretKey != "" {
		q.Set("secret-key", cmd.SecretKey)
	}
	cmd.URL.RawQuery = q.Encode()

	// Perform the request
	return withClient(ctx, "volume-create-s3", func(ctx context.Context, client *httpclient.Client) error {
		volume, err := client.CreateVolume(ctx, schema.VolumeCreate{
			URL: cmd.URL.String(),
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

func (cmd *VolumeReindexCmd) Run(ctx server.Cmd) error {
	// Perform the request
	return withClient(ctx, "volume-reindex", func(ctx context.Context, client *httpclient.Client) error {
		if err := client.ReindexVolume(ctx, cmd.Name, cmd.ObjectListFilters); err != nil {
			return err
		}
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

///////////////////////////////////////////////////////////////////////////////
// GOOGLE CLIENT COMMANDS

type GoogleLoginCmd struct {
	ReadOnly     bool   `name:"read-only" help:"Request read-only access to Google Drive." negatable:""`
	ClientID     string `name:"client-id" help:"Google OAuth2 client ID." env:"GOOGLE_CLIENT_ID"`
	ClientSecret string `name:"client-secret" help:"Google OAuth2 client secret." env:"GOOGLE_CLIENT_SECRET"`
}

func (cmd *GoogleLoginCmd) Run(globals server.Cmd) error {
	logger := globals.Logger()

	// Perform the request
	return withClient(globals, "google-login", func(ctx context.Context, filerclient *httpclient.Client) error {
		return withGoogleClient(globals, "google-login", func(ctx context.Context, client *google.Client) error {
			config, err := client.DiscoverConfig(ctx)
			if err != nil {
				return err
			}

			// Get server metadata for flow
			oauthconfig, err := config.AuthorizationCodeConfig()
			if err != nil {
				return err
			}

			// Create the callback listener first so the resolved loopback URL, including
			// any auto-selected port, is used consistently for registration and the auth flow.
			callback, err := webcallback.New("http://localhost/")
			if err != nil {
				return err
			}

			// Determine scopes for the authorization flow
			scopes := []string{"https://www.googleapis.com/auth/drive"}
			if cmd.ReadOnly {
				scopes = []string{"https://www.googleapis.com/auth/drive.readonly"}
			}
			flow, err := oidc.NewAuthorizationCodeFlow(oauthconfig, cmd.ClientID, callback.URL(), scopes...)
			if err != nil {
				return err
			}

			authURL, err := url.Parse(flow.AuthorizationURL)
			if err != nil {
				return err
			}

			// Google only issues a refresh token when access_type=offline is set.
			// prompt=consent forces re-consent so a refresh token is returned even
			// if the user has previously authorized this client.
			q := authURL.Query()
			q.Set("access_type", "offline")
			q.Set("prompt", "consent")
			authURL.RawQuery = q.Encode()
			flow.AuthorizationURL = authURL.String()

			// In parallel, open the browser to the authorization URL and wait for the callback to be received,
			// then exchange the code for a token and store it
			g, groupCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				result, err := callback.Run(groupCtx)
				if err != nil {
					return err
				}
				code, err := flow.ValidateCallback(result.Query.Get("code"), result.Query.Get("state"))
				if err != nil {
					return err
				}
				token, err := client.ExchangeCode(groupCtx, flow, code, cmd.ClientSecret)
				if err != nil {
					return err
				}
				fmt.Println(types.Stringify(token))

				credential, err := filerclient.CreateCredential(groupCtx, schema.CredentialCreate{
					CredentialKey: schema.CredentialKey{
						Key: "google",
					},
					Credentials: token,
				})
				if err != nil {
					return err
				}

				logger.DebugContext(ctx, "Stored credential", "credential", credential)
				return nil
			})
			g.Go(func() error {
				logger.DebugContext(ctx, "Opening browser for authorization code flow", "url", flow.AuthorizationURL)
				return browser.OpenURL(flow.AuthorizationURL)
			})
			if err := g.Wait(); err != nil {
				return err
			}
			return nil
		})
	})
}
