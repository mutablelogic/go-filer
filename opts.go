package filer

import (
	// Packages
	credential "github.com/mutablelogic/go-auth/credential"
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

	// Credentials are the server's, which a backend looks up of the names in
	// its URL, such as S3_SECRET, or are its sign-in, such as Google's, when
	// it's set
	Credentials credential.CredentialProvider

	// ClientOpts are the options of the clients of a backend which calls
	// other services over HTTP, such as Google's, which it's given as they
	// are, such as a user agent, a tracer, of client.OptTracer, a trace, or
	// a timeout
	ClientOpts []client.ClientOpt
}
