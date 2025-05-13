package main

import (
	// Packages
	llm "github.com/mutablelogic/go-filer/pkg/llm/config"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Plugin() server.Plugin {
	return llm.Config{}
}
