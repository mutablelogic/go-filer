package httpclient

import (
	"io"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// putPayload implements client.Payload for PUT requests with a body.
type putPayload struct {
	body        io.Reader
	contentType string
}

var _ client.Payload = (*putPayload)(nil)

///////////////////////////////////////////////////////////////////////////////
// INTERFACE IMPLEMENTATION

func (p *putPayload) Method() string {
	return http.MethodPut
}

func (p *putPayload) Accept() string {
	return types.ContentTypeJSON
}

func (p *putPayload) Type() string {
	if p.contentType != "" {
		return p.contentType
	}
	return types.ContentTypeBinary
}

func (p *putPayload) Read(b []byte) (int, error) {
	return p.body.Read(b)
}
