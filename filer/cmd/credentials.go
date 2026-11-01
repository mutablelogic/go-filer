//go:build !client

package cmd

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// envCredentials is a provider of credentials from the environment of the
// server, such as S3_SECRET, and those which are stored while it runs, which
// replace them, and which aren't written to the environment
type envCredentials struct {
	sync.RWMutex
	stored map[string]string
}

var _ gofiler.CredentialProvider = (*envCredentials)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newEnvCredentials returns a provider of credentials from the environment
func newEnvCredentials() *envCredentials {
	return &envCredentials{stored: make(map[string]string)}
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Expand substitutes the credentials of the placeholders of the value, such
// as $S3_SECRET or ${S3_SECRET}. A credential which isn't stored, or set in
// the environment, is ErrNotFound, rather than being empty.
func (self *envCredentials) Expand(_ context.Context, value string) (string, error) {
	var err error
	result := os.Expand(value, func(key string) string {
		credential, exists := self.lookup(key)
		if !exists && err == nil {
			err = gofiler.ErrNotFound.Withf("credential %q isn't set", key)
		}
		return credential
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// Get returns the credential for the key, as a JSON string, or ErrNotFound
func (self *envCredentials) Get(_ context.Context, key string) (json.RawMessage, error) {
	credential, exists := self.lookup(key)
	if !exists {
		return nil, gofiler.ErrNotFound.Withf("credential %q isn't set", key)
	}
	return json.Marshal(credential)
}

// Set stores the credential for the key, which is a JSON string, while the
// server runs, in place of the environment's
func (self *envCredentials) Set(_ context.Context, key string, value json.RawMessage) error {
	var credential string
	if err := json.Unmarshal(value, &credential); err != nil {
		return gofiler.ErrBadParameter.Withf("credential %q isn't a string: %v", key, err)
	}
	self.Lock()
	defer self.Unlock()
	self.stored[key] = credential
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// lookup returns the credential which is stored, or set in the environment
func (self *envCredentials) lookup(key string) (string, bool) {
	self.RLock()
	credential, exists := self.stored[key]
	self.RUnlock()
	if exists {
		return credential, true
	}
	return os.LookupEnv(key)
}
