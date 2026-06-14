package mime

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// EtagForPath returns a deterministic etag for the file contents at path.
// The value is a lowercase SHA-256 hex digest.
func EtagForPath(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
