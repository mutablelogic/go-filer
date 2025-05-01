package main

import (
	"fmt"
	"os"

	// Packages
	filer "github.com/mutablelogic/go-filer/pkg/filer/cmd"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	ServiceCommands
	filer.BucketCommands
	VersionCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	// Parse command-line flags
	var cli CLI

	app, err := cmd.New(&cli, "go-filer command-line tool")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}

	if err := app.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-2)
	}
}
