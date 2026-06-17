package cmd

import (
	"context"
	"fmt"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

const defaultVolumeKey = "volume"

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	Volumes  BackendsCommand `cmd:"" name:"volumes" help:"Return list of volumes." group:"SERVER"`
	List     ListCommand     `cmd:"" name:"ls" help:"List objects." group:"OBJECTS"`
	Get      GetCommand      `cmd:"" name:"cat" help:"Download an object." group:"OBJECTS"`
	Head     HeadCommand     `cmd:"" name:"head" help:"Get object metadata." group:"OBJECTS"`
	Delete   DeleteCommand   `cmd:"" name:"rm" help:"Delete an object or prefix." group:"OBJECTS"`
	Upload   UploadCommand   `cmd:"" name:"upload" help:"Upload a file or directory." group:"OBJECTS"`
	Download DownloadCommand `cmd:"" name:"download" help:"Download an object to a file or directory." group:"OBJECTS"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Client returns an httpclient.Client configured from the global HTTP flags.
func Client(ctx server.Cmd) (*httpclient.Client, error) {
	url, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return nil, err
	}
	return httpclient.New(url, opts...)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func withClient(globals server.Cmd, command string, attrs any, fn func(context.Context, *httpclient.Client) error) (err error) {
	// Create the client
	client, err := Client(globals)
	if err != nil {
		return err
	}

	// Create a span
	ctx, endFunc := otel.StartSpan(globals.Tracer(), globals.Context(), command,
		attribute.String("cmd", types.Stringify(attrs)),
	)
	defer func() { endFunc(err) }()

	// Run the command
	return fn(ctx, client)
}

// resolveVolume returns the backend volume to use. Priority:
//  1. explicit — provided via --volume flag; saved as new default via globals.Set
//  2. saved default from globals.GetString
//  3. auto-detect: if the server has exactly one volume, use it and save it
func resolveVolume(ctx context.Context, globals server.Cmd, client *httpclient.Client, explicit string) (string, error) {
	if explicit != "" {
		_ = globals.Set(defaultVolumeKey, explicit)
		return explicit, nil
	}
	if saved := globals.GetString(defaultVolumeKey); saved != "" {
		return saved, nil
	}
	// Auto-detect from server
	resp, err := client.ListBackends(ctx)
	if err != nil {
		return "", err
	}
	if len(resp.Body) == 1 {
		for name := range resp.Body {
			_ = globals.Set(defaultVolumeKey, name)
			return name, nil
		}
	}
	if len(resp.Body) == 0 {
		return "", fmt.Errorf("no volumes registered on the server")
	}
	names := make([]string, 0, len(resp.Body))
	for name := range resp.Body {
		names = append(names, name)
	}
	return "", fmt.Errorf("multiple volumes available (%s); specify one with --volume", strings.Join(names, ", "))
}
