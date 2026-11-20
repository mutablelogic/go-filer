//go:build unix

package registry

import (
	"errors"
	"os"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	unix "golang.org/x/sys/unix"
)

// lockDir locks the file of a directory, which is created, so the directory
// is the registry's alone, or returns ErrConflict, when another has locked it
func lockDir(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); errors.Is(err, unix.EWOULDBLOCK) {
		f.Close()
		return nil, gofiler.ErrConflict.Withf("the cache directory %q is another server's", path)
	} else if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// unlockDir unlocks the file of a directory, and closes it
func unlockDir(f *os.File) error {
	return errors.Join(unix.Flock(int(f.Fd()), unix.LOCK_UN), f.Close())
}
