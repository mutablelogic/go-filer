// Copyright 2026 David Thorpe
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer/cmd"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	version "github.com/mutablelogic/go-server/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	filer.ServerCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := cmd.Main(CLI{}, "File Management Server", version.Version()); err != nil {
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
