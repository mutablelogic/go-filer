package schema

import (
	_ "embed"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	DefaultSchema = "filer"

	// HTTP headers
	ObjectMetaHeader = "X-Object-Meta"

	// prefix for user-defined metadata headers on PUT
	ObjectMetaKeyPrefix = "X-Meta-"

	// AttrLastModified is the metadata key used to store the object modification time.
	// S3 normalizes metadata keys to lowercase, so we use lowercase for consistency.
	AttrLastModified = "last-modified"

	// IndexingQueueName is the name of the queue used by the manager to schedule
	// background tasks for indexing objects.
	IndexingQueueName = "filer-index"

	// Maximum of 15 minutes for each indexing task
	IndexingTTL = time.Minute * 15
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed objects.sql
var Objects string

//go:embed queries.sql
var Queries string
