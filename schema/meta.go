package schema

import "encoding/json"

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Meta is a string key-value map for user-defined metadata.
// Keys should be lowercase for S3 compatibility
type Meta struct {
	Namespace string          `json:"namespace,omitempty"`
	Key       string          `json:"key,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
}
