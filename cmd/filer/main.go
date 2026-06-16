package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	// Packages
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	version "github.com/mutablelogic/go-server/pkg/version"

	// Metadata Extractors
	_ "github.com/mutablelogic/go-filer/metadata/audio"
	_ "github.com/mutablelogic/go-filer/metadata/pdf"
	_ "github.com/mutablelogic/go-filer/metadata/text"
	//_ "github.com/mutablelogic/go-filer/metadata/image"
	//_ "github.com/mutablelogic/go-filer/metadata/markdown"
	//_ "github.com/mutablelogic/go-filer/metadata/srt"
	//_ "github.com/mutablelogic/go-filer/metadata/video"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	filercmd.ServerCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := cmd.Main(CLI{}, "Filer Server", version.Version()); err != nil {
		str, code := formatError(err)
		fmt.Fprintln(os.Stderr, "Error:", str)
		os.Exit(code)
	}
}

func formatError(err error) (string, int) {
	var errResponse httpresponse.ErrResponse
	if errors.As(err, &errResponse) {
		if reason := strings.TrimSpace(errResponse.Reason); reason != "" {
			return reason, errResponse.Code
		}
	}
	return err.Error(), -1
}
