//go:build !client

package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	// Packages
	filecredential "github.com/mutablelogic/go-auth/credential/file"
	gofiler "github.com/mutablelogic/go-filer"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// CredentialFlags are the passphrases which the server's credentials, such
// as the secrets of buckets, and sign-ins, are encrypted with, which it
// keeps in its folder of the user's data
type CredentialFlags struct {
	Passphrase []string `name:"passphrase" env:"${ENV_NAME}_PASSPHRASE" sep:"none" help:"Passphrase which the server's credentials are encrypted with, which can be repeated, for versions 1, 2 and so on, the last of which encrypts new ones. Without one, a passphrase is made, the first time, and kept in credentials.key, beside them, in the server's folder of the user's data."`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// credentialsDir is the directory of the credentials, in the
	// command's folder of the user's data, and credentialsKeyFile the file
	// beside it which has the passphrase they're encrypted with, without
	// --passphrase
	credentialsDir     = "credentials"
	credentialsKeyFile = "credentials.key"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// provider returns the provider of the credentials, which are kept,
// encrypted, in the command's folder of the user's data, or else the
// environment's, such as S3_SECRET, so a volume's placeholder, such as
// ${S3_SECRET}, is either. Those of an earlier version, in its folder of
// the user's cache, and their passphrase, are moved there, when there are
// none there.
func (flags CredentialFlags) provider(ctx server.Cmd) (*filecredential.Manager, error) {
	dir, err := ctx.DataDir()
	if err != nil {
		return nil, err
	}
	if old, err := oldCacheDir(); err != nil {
		return nil, err
	} else {
		for _, name := range []string{credentialsDir, credentialsKeyFile} {
			if err := moveFrom(filepath.Join(old, ctx.Name(), name), filepath.Join(dir, name)); err != nil {
				return nil, err
			}
		}
	}
	// A passphrase which is empty, as a variable which is set, but empty,
	// such as of Docker Compose, is none, unless there are others, whose
	// versions are of their places
	passphrases := flags.Passphrase
	if !slices.ContainsFunc(passphrases, func(p string) bool { return p != "" }) {
		passphrases = nil
	}
	if len(passphrases) == 0 {
		passphrase, err := credentialsKey(filepath.Join(dir, credentialsKeyFile))
		if err != nil {
			return nil, err
		}
		passphrases = []string{passphrase}
	}
	opts := []filecredential.Opt{filecredential.WithEnv(), filecredential.WithTracer(ctx.Tracer())}
	for i, passphrase := range passphrases {
		opts = append(opts, filecredential.WithPassphrase(uint64(i+1), passphrase))
	}
	return filecredential.New(filepath.Join(dir, credentialsDir), opts...)
}

// credentialsKey returns the passphrase in the file, or makes one, of 32
// random bytes, and writes it, readable only by its owner, when there's
// none. It's written to a temporary file, which is linked to the path, so
// a server which starts at the same time reads the same one.
func credentialsKey(path string) (string, error) {
	if passphrase, err := readCredentialsKey(path); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return passphrase, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.WriteString(base64.RawURLEncoding.EncodeToString(data) + "\n"); err != nil {
		temp.Close()
		return "", err
	} else if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return "", err
	} else if err := temp.Close(); err != nil {
		return "", err
	}
	if err := os.Link(temp.Name(), path); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	return readCredentialsKey(path)
}

// readCredentialsKey returns the passphrase in the file, which must only be
// readable by its owner. On Windows, a file's permissions are its ACLs, which
// its mode doesn't say, as it's 0666 unless the file is read-only, so it
// isn't checked, and the file is in the user's local application data,
// whose ACLs only let the user read it, by default.
func readCredentialsKey(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	} else if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", gofiler.ErrBadParameter.Withf("%s is readable by others than its owner", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	passphrase := strings.TrimSpace(string(data))
	if passphrase == "" {
		return "", gofiler.ErrBadParameter.Withf("%s is empty", path)
	}
	return passphrase, nil
}
