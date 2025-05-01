package aws

import (
	"context"
	"net/url"

	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Config struct {
	Region     string   `name:"region" env:"AWS_REGION" help:"AWS region"`
	S3endpoint *url.URL `name:"s3-endpoint" env:"S3_ENDPOINT" help:"S3 endpoint"`
}

var _ server.Plugin = Config{}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (c Config) New(ctx context.Context) (server.Task, error) {
	// Set options
	opts := []aws.Opt{}
	if c.S3endpoint != nil {
		opts = append(opts, aws.WithS3Endpoint(c.S3endpoint.String()))
	}

	// Create a new AWS client
	client, err := aws.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Return an AWS task
	return taskWithClient(client), nil
}

////////////////////////////////////////////////////////////////////////////////
// MODULE

func (Config) Name() string {
	return "aws"
}

func (Config) Description() string {
	return "AWS services"
}
