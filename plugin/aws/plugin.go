package main

import (
	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws/config"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Plugin() server.Plugin {
	return aws.Config{}
}
