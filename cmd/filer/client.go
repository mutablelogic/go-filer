//go:build client

package main

import (
	// Packages
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	filercmd.ClientCommands
}
