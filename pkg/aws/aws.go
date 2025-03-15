package aws

import (
	"context"

	// Packages
	config "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type aws struct {
	region string
	s3     *s3.Client
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, opt ...Opt) (*aws, error) {
	aws := new(aws)
	opts, err := applyOpts(opt...)
	if err != nil {
		return nil, err
	}

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
		} else {
			aws.region = o.Region
		}

		// We set the endpoint if it is not empty
		if opts.s3endpoint != nil {
			o.BaseEndpoint = opts.s3endpoint
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

func (*aws) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (aws *aws) S3() *s3.Client {
	return aws.s3
}

func (aws *aws) Region() string {
	return aws.region
}
