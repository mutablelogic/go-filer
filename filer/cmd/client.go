package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	credentialcmd "github.com/mutablelogic/go-filer/credential/cmd"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadataMime "github.com/mutablelogic/go-filer/metadata/mime"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	ObjectClientCommands
	SearchClientCommands
	VolumeClientCommands
	MetadataClientCommands
	ArtworkClientCommands
	LLMProviderClientCommands
	GoogleClientCommands
	credentialcmd.ClientCommands
}

type ObjectClientCommands struct {
	ObjectList ObjectListCmd `cmd:"" name:"objects" help:"List server objects." group:"OBJECT"`
	ObjectGet  ObjectGetCmd  `cmd:"" name:"object" help:"Get object metadata by volume and path." group:"OBJECT"`
	ObjectPut  ObjectPutCmd  `cmd:"" name:"object-upload" help:"Upload an object to a volume." group:"OBJECT"`
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

type LLMProviderClientCommands struct {
	LLMProviderCreate LLMProviderCreateCmd `cmd:"" name:"llm-create" help:"Create or update an LLM provider." group:"LLM PROVIDER"`
}

type SearchCmd struct {
	schema.SearchListRequest
}

type MetadataCmd struct {
	Path string `arg:"" name:"path" type:"file" help:"Path to the local file."`
}

type LLMProviderCreateCmd struct {
	schema.LLMProviderCreate
}

type GoogleClientCommands struct {
	GoogleLogin GoogleLoginCmd `cmd:"" name:"google-login" help:"Login to the Google." group:"GOOGLE"`
}

type progressReader struct {
	r        io.Reader
	total    int64
	written  int64
	lastEmit int64
	cb       func(written, total int64)
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

func newProgressReader(r io.Reader, total int64, cb func(written, total int64)) io.Reader {
	return &progressReader{r: r, total: total, cb: cb}
}

func (r *progressReader) Read(data []byte) (int, error) {
	n, err := r.r.Read(data)
	if n > 0 {
		r.written += int64(n)
		if r.written-r.lastEmit >= 64*1024 || (r.total > 0 && r.written >= r.total) {
			r.lastEmit = r.written
			r.cb(r.written, r.total)
		}
	}
	return n, err
}

///////////////////////////////////////////////////////////////////////////////
// OBJECT COMMANDS

type ObjectListCmd struct {
	schema.ObjectListRequest
}

type ObjectGetCmd struct {
	schema.ObjectKey
}

type ObjectPutCmd struct {
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

func (cmd *ObjectPutCmd) Run(ctx server.Cmd) error {
	width := ctx.IsTerm()
	info, err := os.Stat(cmd.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return gofiler.ErrNotFound.Withf("object not found: %q", cmd.Path)
		}
		return err
	} else if info.IsDir() {
		return gofiler.ErrBadParameter.Withf("object path is a directory: %q", cmd.Path)
	}

	object := schema.ObjectKey{
		Volume: cmd.Volume,
		Path:   objectUploadPath(cmd.Path),
	}
	attr := schema.ObjectAttr{
		Size:    types.Ptr(info.Size()),
		ModTime: info.ModTime(),
	}
	contentType := metadataMime.TypeByExtension(filepath.Ext(cmd.Path))
	bar := tui.Progress(tui.SetWidth(width))
	progress := func(written, total int64) {
		if width == 0 {
			return
		}
		percent := float64(100)
		if total > 0 {
			percent = float64(written) * 100 / float64(total)
		}
		fmt.Fprint(os.Stderr, "\r\x1b[K")
		_, _ = bar.Write(os.Stderr, fmt.Sprintf("%s %d/%d", object.Path, written, total), percent)
	}

	// Perform the request
	return withClient(ctx, "object", func(ctx context.Context, client *httpclient.Client) error {
		f, err := os.Open(cmd.Path)
		if err != nil {
			if os.IsNotExist(err) {
				return gofiler.ErrNotFound.Withf("object not found: %q", cmd.Path)
			}
			return err
		}
		defer f.Close()

		progress(0, info.Size())
		uploaded, err := client.PutObject(ctx, object.Volume, object.Path, newProgressReader(f, info.Size(), progress), attr, contentType)
		if err != nil {
			if errors.Is(err, httpresponse.Err(http.StatusNotModified)) {
				if width > 0 {
					fmt.Fprint(os.Stderr, "\r\x1b[K")
				}
				uploaded, err = client.GetObject(ctx, object.Volume, object.Path)
				if err != nil {
					return err
				}
				fmt.Println(uploaded)
				return nil
			}
			return err
		}
		if width > 0 {
			fmt.Fprint(os.Stderr, "\r\x1b[K")
		}

		fmt.Println(uploaded)
		return nil
	})
}

func objectUploadPath(localPath string) string {
	path := filepath.ToSlash(localPath)
	if index := strings.Index(path, "/./"); index >= 0 {
		path = path[index+3:]
	} else if strings.HasPrefix(path, "./") {
		path = strings.TrimPrefix(path, "./")
	}
	return strings.TrimPrefix(types.NormalisePath(path), "/")
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
	Region    string   `name:"region" help:"AWS region. Defaults to us-east-1 for custom endpoints."`
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
	Force bool `json:"force" short:"f" help:"Force reindexing of all objects, even if they are already indexed"`
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
	if cmd.Region != "" {
		q.Set("region", cmd.Region)
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
		return client.ReindexVolume(ctx, cmd.Name, cmd.ObjectListFilters, cmd.Force, func(total uint64) {
			fmt.Println("Reindexing:", total, "objects considered")
		})
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

/*
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
*/
