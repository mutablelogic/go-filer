package schema

import (
	"time"

	// Packages
	"github.com/google/uuid"
	"github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeKey string

type VolumeMeta struct {
	Name       string         `json:"name,omitempty"`
	Enabled    *bool          `json:"enabled,omitempty"`
	IndexDelta *time.Duration `json:"findex_delta,omitempty"` // if set, forces a full re-index if the last index is older than this duration
}

type Volume struct {
	VolumeKey uuid.UUID `json:"id,omitempty"`
	VolumeMeta
	CreatedAt time.Time `json:"created_at,omitempty"`
	IndexedAt time.Time `json:"indexed_at,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (v Volume) String() string {
	return types.Stringify(v)
}

func (v VolumeMeta) String() string {
	return types.Stringify(v)
}
