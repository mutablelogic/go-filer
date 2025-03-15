package schema

import (
	"time"

	// Packages
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Bucket struct {
	Name   string    `json:"name,omitempty" name:"name" help:"Name of the bucket"`
	Ts     time.Time `json:"ts,omitzero" name:"ts" help:"Creation date of the bucket"`
	Region *string   `json:"region,omitempty" name:"region" help:"Region of the bucket"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func BucketFromAWS(bucket *s3types.Bucket) *Bucket {
	return &Bucket{
		Name:   types.PtrString(bucket.Name),
		Ts:     types.PtrTime(bucket.CreationDate),
		Region: bucket.BucketRegion,
	}
}
