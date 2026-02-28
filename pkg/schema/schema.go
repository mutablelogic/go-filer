package schema

////////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	SchemaName = "filer"

	// HTTP headers
	ObjectMetaHeader    = "X-Object-Meta"
	ObjectMetaKeyPrefix = "X-Meta-" // prefix for user-defined metadata headers on PUT

	// AttrLastModified is the metadata key used to store the object modification time.
	// S3 normalizes metadata keys to lowercase, so we use lowercase for consistency.
	AttrLastModified = "last-modified"
)
