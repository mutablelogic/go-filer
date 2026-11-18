package filer

import (
	// Packages
	client "github.com/mutablelogic/go-client"
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
	// backend, such as ${S3_SECRET}, or are its sign-in, such as Google's,
	// when it's set
	Credentials CredentialProvider

	// ClientOpts are the options of the clients of a backend which calls
	// other services over HTTP, such as Google's, which it's given as they
	// are, such as a user agent, a tracer, of client.OptTracer, a trace, or
	// a timeout
	ClientOpts []client.ClientOpt
}
