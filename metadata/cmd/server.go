//go:build !client

// Package cmd is the flags of the metadata enrichment of a server, which it embeds,
// and composes into the registry of its volumes
package cmd

import (
	"context"
	"log/slog"

	// Packages
	client "github.com/mutablelogic/go-client"
	metadata "github.com/mutablelogic/go-filer/metadata"
	registry "github.com/mutablelogic/go-filer/registry"
	chromaprint "github.com/mutablelogic/go-metadata/pkg/chromaprint"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Flags are the options of the task which enriches the files of volumes with
// index=metadata, with their metadata, of go-metadata
type Flags struct {
	TMDBKey     string `name:"tmdb-key" env:"TMDB_TOKEN" help:"The API Read Access Token of TMDB, with which videos are looked up, as movies, or TV episodes, when they're enriched. Set it in the environment, rather than as a flag, to keep it out of the shell's history."`
	AcoustIDKey string `name:"acoustid-key" env:"ACOUSTID_KEY" help:"The API key of AcoustID, with which audio is looked up, as recordings, when it's enriched. Set it in the environment, rather than as a flag, to keep it out of the shell's history."`
	MaxSize     int64  `name:"enrich-max-size" placeholder:"BYTES" help:"The most of a file which is read when it's enriched, in bytes, or zero for no limit." default:"0"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithMetadataEnrichment returns the options of the registry which enrich
// the files of its volumes with index=metadata, with their metadata, of the
// flags, the server's tracer, and a user agent of its name and version, of
// the requests to TMDB and AcoustID, and which read files as Markdown
func (flags Flags) WithMetadataEnrichment(ctx server.Cmd) ([]registry.Opt, error) {
	opts := []metadata.Opt{
		metadata.WithTracer(ctx.Tracer()),
		metadata.WithClientOpts(client.OptUserAgent(ctx.Name() + "/" + ctx.Version())),
	}
	if flags.TMDBKey != "" {
		opts = append(opts, metadata.WithTMDB(flags.TMDBKey))
	}
	if flags.AcoustIDKey != "" {
		opts = append(opts, metadata.WithAcoustID(flags.AcoustIDKey))
	}
	if flags.MaxSize > 0 {
		opts = append(opts, metadata.WithMaxSize(flags.MaxSize))
	}
	e, err := metadata.New(opts...)
	if err != nil {
		return nil, err
	}
	flags.logLookups(ctx.Context(), ctx.Logger(), chromaprint.Enabled)
	return []registry.Opt{registry.WithMetadataTask(e.Enrich), registry.WithMarkdown(e.Markdown)}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// logLookups logs which of the online lookups of enrichment are on, once,
// as the server starts: TMDB, of videos, and AcoustID, of audio, which
// fingerprints it with Chromaprint, when it's built in, and a warning of
// each which is off, and how it's turned on
func (flags Flags) logLookups(ctx context.Context, logger *slog.Logger, fingerprints bool) {
	if flags.TMDBKey != "" {
		logger.InfoContext(ctx, "TMDB lookups are on: videos are looked up as films and TV episodes")
	} else {
		logger.WarnContext(ctx, "TMDB lookups are off: set TMDB_TOKEN to look videos up as films and TV episodes")
	}
	switch {
	case flags.AcoustIDKey == "":
		logger.WarnContext(ctx, "AcoustID lookups are off: set ACOUSTID_KEY to look audio up as recordings")
	case fingerprints:
		logger.InfoContext(ctx, "AcoustID lookups are on: audio is looked up as recordings, of its Chromaprint fingerprints")
	default:
		logger.WarnContext(ctx, "AcoustID lookups are on, but Chromaprint isn't built in: audio is only looked up of the AcoustID identifiers in its tags, as it can't be fingerprinted without cgo and the chromaprint tag")
	}
}
