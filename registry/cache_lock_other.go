//go:build !unix

package registry

import (
	"os"
)

// lockDir doesn't lock the directory, where files can't be, so it's up to
// the servers not to share it
func lockDir(string) (*os.File, error) {
	return nil, nil
}

// unlockDir does nothing, as the directory isn't locked
func unlockDir(*os.File) error {
	return nil
}
