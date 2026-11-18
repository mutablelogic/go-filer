//go:build !client

package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	// Packages
	filecredential "github.com/mutablelogic/go-auth/credential/file"
	gofiler "github.com/mutablelogic/go-filer"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// CredentialFlags are where the server keeps the volumes' credentials, such
// as the secrets of buckets, and sign-ins, encrypted, and the passphrases
// they're encrypted with
type CredentialFlags struct {
	Credentials string   `name:"credentials" env:"${ENV_NAME}_CREDENTIALS" placeholder:"DIR" help:"Directory of the server's credentials, encrypted, such as the secrets of buckets and sign-ins. Defaults to credentials in the user's cache folder."`
	Passphrase  []string `name:"passphrase" env:"${ENV_NAME}_PASSPHRASE" sep:"none" help:"Passphrase which the credentials are encrypted with, which can be repeated, for versions 1, 2 and so on, the last of which encrypts new ones. Without one, a passphrase is made, the first time, and kept beside the directory, in credentials.key."`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// credentialsDir is the directory of the credentials, in the
	// command's folder of the user's cache, unless --credentials sets
	// another, and credentialsKeyFile the file beside it which has the
	// passphrase they're encrypted with, without --passphrase
	credentialsDir     = "credentials"
	credentialsKeyFile = "credentials.key"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// provider returns the provider of the credentials, which are kept,
// encrypted, in the directory, or else the environment's, such as
// S3_SECRET, so a volume's placeholder, such as ${S3_SECRET}, is either
func (flags CredentialFlags) provider(ctx server.Cmd) (*filecredential.Manager, error) {
	dir := flags.Credentials
	if dir == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return nil, err
		}
		dir = filepath.Join(cache, ctx.Name(), credentialsDir)
	}
	passphrases := flags.Passphrase
	if len(passphrases) == 0 {
		passphrase, err := credentialsKey(filepath.Join(filepath.Dir(filepath.Clean(dir)), credentialsKeyFile))
		if err != nil {
			return nil, err
		}
		passphrases = []string{passphrase}
	}
	opts := []filecredential.Opt{filecredential.WithEnv(), filecredential.WithTracer(ctx.Tracer())}
	for i, passphrase := range passphrases {
		opts = append(opts, filecredential.WithPassphrase(uint64(i+1), passphrase))
	}
	return filecredential.New(dir, opts...)
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
// readable by its owner
func readCredentialsKey(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	} else if info.Mode().Perm()&0o077 != 0 {
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
