package handler

import (
	"encoding/json"
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectHead(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket, key string) error {
	// Get object metadata
	object, err := filer.GetObject(r.Context(), bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, types.JoinPath(bucket, key))
	}

	// Add the JSON metadata to a header
	json, err := json.Marshal(object)
	if err != nil {
		return httpresponse.Error(w, err)
	} else {
		w.Header().Set(schema.HeaderMetaKey, string(json))
	}

	return httpresponse.Empty(w, http.StatusOK)
}
