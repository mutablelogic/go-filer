package aws

import (
	"context"
	"net/url"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	config "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Config struct {
	S3endpoint *url.URL `name:"s3-endpoint" env:"S3_ENDPOINT" help:"S3 endpoint"`
}

var _ server.Plugin = Config{}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (c Config) New(ctx context.Context) (server.Task, error) {
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
// MODULE

func (Config) Name() string {
	return "aws"
}

func (Config) Description() string {
	return "AWS services"
}
