//go:build client

package main

import (
	// Packages
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// CLI is the client, which has the commands which call a server, but not
// the commands which run a server. It's built with the "client" tag, which
// leaves the server out of the command packages too.
type CLI struct {
	Filer filercmd.ClientCommands `embed:""`
}
