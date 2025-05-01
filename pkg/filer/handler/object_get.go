package handler

import (
	"net/http"
	"net/url"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectHead(w http.ResponseWriter, r *http.Request, filer filer.AWS, bucket, key string) error {
	var meta url.Values

	// Callback for metadata
	metafn := func(v url.Values) error {
		meta = v
		return nil
	}

	// Get object metadata
	object, err := filer.GetObject(r.Context(), nil, metafn, bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, types.JoinPath(bucket, key))
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.ObjectFromAWS(object, bucket, meta))
}
