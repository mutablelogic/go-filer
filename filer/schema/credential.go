package schema

import (
	"strings"
	"time"

	// Packages

	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CredentialKey struct {
	Key string `json:"key" help:"Credential key"`
}

// Credential is the public credential row returned from the database.
// Secret material and passphrase version are intentionally excluded.
type Credential struct {
	CredentialKey
	UpdatedAt time.Time `json:"updated_at" help:"Update timestamp" readonly:""`
}

// CredentialInsert contains the values required to insert a credential row.
// The returned Credential omits PV and Credentials.
type CredentialInsert struct {
	CredentialKey
	Credentials []byte `json:"credentials" help:"Encrypted credential payload"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (c Credential) String() string {
	return types.Stringify(c)
}

func (c CredentialInsert) String() string {
	return types.Stringify(c)
}

func (c CredentialInsert) RedactedString() string {
	r := c
	r.Credentials = nil
	return types.Stringify(r)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - READER

// Expected column order: key, updated_at.
func (c *Credential) Scan(row pg.Row) error {
	if err := row.Scan(&c.Key, &c.UpdatedAt); err != nil {
		return err
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - WRITER

func (c CredentialInsert) Insert(bind *pg.Bind) (string, error) {
	if key := strings.TrimSpace(c.Key); !types.IsIdentifier(key) {
		return "", gofiler.ErrBadParameter.With("credential key must be a non-empty identifier")
	} else {
		bind.Set("key", key)
	}

	if c.Credentials == nil {
		return "", gofiler.ErrBadParameter.With("credential credentials are required")
	} else if !bind.Has("pv") {
		return "", gofiler.ErrInternalServerError.With("credential insert requires passphrase version binding")
	} else {
		bind.Set("credentials", c.Credentials)
	}
	return bind.Query("credential.upsert"), nil
}

func (c CredentialInsert) Update(_ *pg.Bind) error {
	return gofiler.ErrNotImplemented.With("CredentialInsert: update: not supported")
}
