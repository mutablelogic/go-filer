package schema

import (
	"encoding/json"
	"time"

	// Packages
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectMeta struct {
	Key string `json:"key,omitempty" name:"key" help:"Object key"`
}

type Object struct {
	ObjectMeta
	Hash   *string   `json:"hash,omitempty" name:"hash" help:"Hash of the object"`
	Bucket string    `json:"bucket,omitempty" name:"bucket" help:"Bucket in which the object is stored"`
	Size   int64     `json:"size" name:"size" help:"Size of the object in bytes"`
	Ts     time.Time `json:"ts,omitzero" name:"ts" help:"Creation date of the object"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func ObjectFromAWS(object *s3types.Object, bucket string) *Object {
	return &Object{
		ObjectMeta: ObjectMeta{
			Key: types.PtrString(object.Key),
		},
		Hash:   object.ETag,
		Bucket: bucket,
		Size:   types.PtrInt64(object.Size),
		Ts:     types.PtrTime(object.LastModified),
	}
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectMeta) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
