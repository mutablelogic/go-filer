package schema

import (
	_ "embed"
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	DefaultSchema = "filer"
	NotifyChannel = "filer_changes"
)

const (
	VolumeListLimit     = 100
	ObjectListLimit     = 100
	CredentialListLimit  = 100
	MetadataListLimit    = 100
	LLMProviderListLimit = 100
	SearchListLimit      = 25

	// MaxUploadFiles is the maximum number of files accepted in a single multipart upload request.
	MaxUploadFiles = 1000
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed objects.sql
var Objects string

//go:embed queries.sql
var Queries string
