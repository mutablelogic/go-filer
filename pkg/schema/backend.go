package schema

import "github.com/mutablelogic/go-server/pkg/types"

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BackendListResponse struct {
	Body map[string]string `json:"body"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (r BackendListResponse) String() string {
	return types.Stringify(r)
}
