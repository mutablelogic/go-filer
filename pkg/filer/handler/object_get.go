package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ObjectGet(w http.ResponseWriter, r *http.Request, client plugin.AWS, bucket, key string) error {
	object, meta, err := client.GetObjectMeta(r.Context(), bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, types.JoinPath(bucket, key))
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.ObjectFromAWS(object, bucket, meta))
}
