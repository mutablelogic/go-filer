package httpclient

import (
	"encoding/json"
	"io"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// readObjectUnmarshaler streams the response body to fn in chunks and captures
// the object metadata from the X-Object-Meta response header.
type readObjectUnmarshaler struct {
	obj *schema.Object
	fn  func([]byte) error
}

var _ client.Unmarshaler = (*readObjectUnmarshaler)(nil)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - READ OBJECT

func (r *readObjectUnmarshaler) Unmarshal(header http.Header, reader io.Reader) error {
	if metaJSON := header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &obj); err != nil {
			return err
		}
		r.obj = &obj
	}
	if r.fn == nil {
		_, err := io.Copy(io.Discard, reader)
		return err
	}
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if callErr := r.fn(buf[:n]); callErr != nil {
				return callErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
