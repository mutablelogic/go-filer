//go:build !client

package main

import (
	// Packages
	client "github.com/mutablelogic/go-client"
	enrichment "github.com/mutablelogic/go-filer/enrichment"
	registry "github.com/mutablelogic/go-filer/registry"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// EnrichmentFlags are the options of the task which enriches the files of
// volumes with index=metadata, with their metadata, of go-metadata
type EnrichmentFlags struct {
	TMDBKey     string `name:"tmdb-key" env:"TMDB_TOKEN" help:"The API Read Access Token of TMDB, with which videos are looked up, as movies, or TV episodes, when they're enriched. Set it in the environment, rather than as a flag, to keep it out of the shell's history."`
	AcoustIDKey string `name:"acoustid-key" env:"ACOUSTID_KEY" help:"The API key of AcoustID, with which audio is looked up, as recordings, when it's enriched. Set it in the environment, rather than as a flag, to keep it out of the shell's history."`
	MaxSize     int64  `name:"enrich-max-size" placeholder:"BYTES" help:"The most of a file which is read when it's enriched, in bytes, or zero for no limit." default:"0"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// task returns the option of the registry which enriches the files of
// volumes with index=metadata, with the flags, and the server's tracer
func (flags EnrichmentFlags) task(ctx server.Cmd) (registry.Opt, error) {
	opts := []enrichment.Opt{
		enrichment.WithTracer(ctx.Tracer()),
		enrichment.WithClientOpts(client.OptUserAgent(ctx.Name() + "/" + ctx.Version())),
	}
	if flags.TMDBKey != "" {
		opts = append(opts, enrichment.WithTMDB(flags.TMDBKey))
	}
	if flags.AcoustIDKey != "" {
		opts = append(opts, enrichment.WithAcoustID(flags.AcoustIDKey))
	}
	if flags.MaxSize > 0 {
		opts = append(opts, enrichment.WithMaxSize(flags.MaxSize))
	}
	e, err := enrichment.New(opts...)
	if err != nil {
		return nil, err
	}
	return registry.WithMetadataTask(e.Enrich), nil
}
