package aws

import (
	"context"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	config "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// INTERFACE

type AWSTask interface {
	server.Task

	// AWS Services
	S3() *s3.Client
}

////////////////////////////////////////////////////////////////////////////////
// TYPES

type task struct {
	s3 *s3.Client
}

var _ server.Task = (*task)(nil)
var _ AWSTask = (*task)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, c Config) (*task, error) {
	self := new(task)

	// Load the default configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	// Create the S3 client
	if s3 := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true

		// If there is no region set, we need to set the credentials to nil
		if o.Region == "" {
			o.Credentials = nil
			o.Region = "none"
		}

		// We set the endpoint if it is not empty
		if c.S3endpoint != nil {
			o.BaseEndpoint = aws.String(c.S3endpoint.String())
		}
	}); s3 == nil {
		return nil, httpresponse.ErrInternalError.Withf("Invalid S3 client")
	} else {
		self.s3 = s3
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// TASK

func (*task) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *task) S3() *s3.Client {
	return t.s3
}
