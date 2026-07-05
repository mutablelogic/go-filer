//go:build !client

package main

import (
	// Packages
	credcmd "github.com/mutablelogic/go-filer/credential/cmd"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	credcmd.ServerCommands
	credcmd.ClientCommands
}
