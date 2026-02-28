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

// getObjectResponse holds the metadata-only response from a GetObject (HEAD) request.
type getObjectResponse struct {
	Object *schema.Object
}

var _ client.Unmarshaler = (*getObjectResponse)(nil)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - GET OBJECT

func (r *getObjectResponse) Unmarshal(header http.Header, _ io.Reader) error {
	if metaJSON := header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &obj); err != nil {
			return err
		}
		r.Object = &obj
	}
	return nil
}
