package filer

import (
	// Packages
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opts are the options of a backend, which the registry passes to it when
// it's created
type Opts struct {
	// Tracer traces the requests of the backend to other services, such as
	// S3, when it's set
	Tracer trace.Tracer

	// Credentials expand the placeholders of the credentials in the URL of a
	// backend, such as ${S3_SECRET}, when it's set
	Credentials CredentialProvider
}
