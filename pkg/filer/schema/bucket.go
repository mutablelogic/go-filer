package schema

import (
	"encoding/json"
	"time"

	// Packages
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	pg "github.com/djthorpe/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type BucketMeta struct {
	Name   string  `json:"name,omitempty" arg:"" name:"name" help:"Name of the bucket"`
	Region *string `json:"region,omitempty" name:"region" help:"Region of the bucket"`
}

type Bucket struct {
	BucketMeta
	Ts time.Time `json:"ts,omitzero" name:"ts" help:"Creation date of the bucket"`
}

type BucketListRequest struct {
	pg.OffsetLimit
}

type BucketList struct {
	Count uint64    `json:"count"`
	Body  []*Bucket `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func BucketFromAWS(bucket *s3types.Bucket) *Bucket {
	return &Bucket{
		BucketMeta: BucketMeta{
			Name:   types.PtrString(bucket.Name),
			Region: bucket.BucketRegion,
		},
		Ts: types.PtrTime(bucket.CreationDate),
	}
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (b Bucket) String() string {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (b BucketMeta) String() string {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (b BucketListRequest) String() string {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (b BucketList) String() string {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
