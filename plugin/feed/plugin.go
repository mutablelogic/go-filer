package main

import (
	// Packages
	feed "github.com/mutablelogic/go-filer/pkg/feed/config"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Plugin() server.Plugin {
	return feed.Config{}
}
