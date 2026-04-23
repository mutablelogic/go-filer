package schema

import (
	"errors"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	SchemaName = "filer"

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

// ErrAlreadyExists is returned by CreateObject when IfNotExists is true and the
// object already exists. It is a distinct sentinel so callers and tracing
// instrumentation can differentiate an intentional conditional-create miss from
// an unexpected conflict.
var (
	ErrAlreadyExists = errors.New("object already exists")
)
