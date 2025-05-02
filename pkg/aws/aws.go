package aws

import (
	"context"

	// Packages
	config "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Client struct {
	region string
	s3     *s3.Client
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, opt ...filer.Opt) (*Client, error) {
	aws := new(Client)
	opts, err := filer.ApplyOpts(opt...)
	if err != nil {
		return nil, err
	}

	// Load the default configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	} else if opts.Region() != "" {
		cfg.Region = opts.Region()
	}

	// Create the S3 client
	if s3 := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true

		// If there is no region set, we need to set the credentials to nil
		if o.Region == "" {
			o.Credentials = nil
			o.Region = "none"
		} else {
			aws.region = o.Region
		}

		// We set the endpoint if it is not empty
		if opts.S3Endpoint() != nil {
			o.BaseEndpoint = opts.S3Endpoint()
		}
	}); s3 == nil {
		return nil, httpresponse.ErrInternalError.Withf("Invalid S3 client")
	} else {
		aws.s3 = s3
	}

	// Return success
	return aws, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (aws *Client) S3() *s3.Client {
	return aws.s3
}

func (aws *Client) Region() string {
	return aws.region
}
