package schema

import (
	_ "embed"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	DefaultSchema     = "filer"
	MetadataListLimit = 100
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	// MaxListLimit is the maximum number of objects that can be returned in a
	// single ListObjects call. Clients must paginate using Offset for larger sets.
	MaxListLimit = 1000

	// MaxUploadFiles is the maximum number of files accepted in a single multipart upload request.
	MaxUploadFiles = 1000
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed objects.sql
var Objects string

//go:embed queries.sql
var Queries string
