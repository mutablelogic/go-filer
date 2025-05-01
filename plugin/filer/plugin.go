package main

import (
	// Packages
	filer "github.com/mutablelogic/go-filer/pkg/filer/config"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Plugin() server.Plugin {
	return filer.Config{}
}
