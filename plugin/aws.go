package plugin

import (
	// Packages
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// INTERFACES

type AWS interface {
	server.Task

	// Return region
	Region() string

	// AWS Services
	S3() *s3.Client
}
