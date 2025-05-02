package handler

import (
	"context"
	"encoding/json"
	"fmt"
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
	_, err := objectHeadInner(r.Context(), w, filer, bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, []string{bucket, key})
	}
	return httpresponse.Empty(w, http.StatusOK)
}

func objectGet(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket, key string) error {
	object, err := objectHeadInner(r.Context(), w, filer, bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, []string{bucket, key})
	}

	// Set headers
	w.Header().Set(types.ContentTypeHeader, object.Type)
	w.Header().Set(types.ContentLengthHeader, fmt.Sprint(object.Size))
	w.Header().Set(types.ContentModifiedHeader, object.Ts.Format(http.TimeFormat))
	if object.Hash != nil {
		w.Header().Set(types.ContentHashHeader, types.PtrString(object.Hash))
	}

	// Write the object to the response
	_, err = filer.WriteObject(r.Context(), w, object.Bucket, object.Key)
	if err != nil {
		return httpresponse.Error(w, err, []string{bucket, key})
	} else {
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectHeadInner(ctx context.Context, w http.ResponseWriter, filer filer.Filer, bucket, key string) (*schema.Object, error) {
	// Get object metadata
	object, err := filer.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Add the JSON metadata to a header
	if json, err := json.Marshal(object); err != nil {
		return nil, err
	} else {
		w.Header().Set(schema.HeaderMetaKey, string(json))
	}

	// Return success
	return object, nil
}
