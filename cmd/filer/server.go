//go:build !client

package main

import (

	// Packages
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"

	// Metadata Extractors
	_ "github.com/mutablelogic/go-filer/metadata/audio"
	_ "github.com/mutablelogic/go-filer/metadata/html"
	_ "github.com/mutablelogic/go-filer/metadata/image"
	_ "github.com/mutablelogic/go-filer/metadata/json"
	_ "github.com/mutablelogic/go-filer/metadata/markdown"
	_ "github.com/mutablelogic/go-filer/metadata/pdf"
	_ "github.com/mutablelogic/go-filer/metadata/sourcecode"
	_ "github.com/mutablelogic/go-filer/metadata/srt"
	_ "github.com/mutablelogic/go-filer/metadata/text"
	_ "github.com/mutablelogic/go-filer/metadata/video"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	filercmd.ServerCommands
	filercmd.ClientCommands
}
