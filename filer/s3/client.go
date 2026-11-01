package s3

import (
	"context"
	"errors"
	"net/http"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// defaultRegion is the region of a bucket, when it isn't set, and the
	// configuration has none, which S3 redirects from when it's another
	defaultRegion = "us-east-1"

	// headerBucketRegion is the header of the region of a bucket, which S3
	// returns when it's requested in another
	headerBucketRegion = "X-Amz-Bucket-Region"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// connect creates the client of the bucket, in the region, and checks the
// bucket can be accessed, with the credentials. When the bucket is in
// another region, and the region isn't set, the client is of the bucket's
// region.
func (self *S3) connect(ctx context.Context) error {
	client, err := self.newClient(ctx, self.region)
	if err != nil {
		return err
	}
	region, err := self.ping(ctx, client)
	if err != nil && region != "" && self.region == "" {
		if client, err = self.newClient(ctx, region); err != nil {
			return err
		}
		_, err = self.ping(ctx, client)
	}
	if err != nil {
		return err
	}

	// Return success
	self.client = client
	return nil
}

// newClient returns a client in the region, or the configuration's, with
// the credentials of the profile, or the default ones, such as from the
// environment, or none, when it's anonymous. It's ErrForbidden when there
// are no credentials.
func (self *S3) newClient(ctx context.Context, region string) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	if self.profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(self.profile))
	}
	if self.anonymous {
		opts = append(opts, awsconfig.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("Bucket %q: %v", self.bucket, err)
	}
	if config.Region == "" {
		config.Region = defaultRegion
	}

	// Get the credentials, which are cached, so it's known when there are
	// none, rather than the request failing
	if !self.anonymous {
		if _, err := config.Credentials.Retrieve(ctx); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, gofiler.ErrForbidden.Withf("Bucket %q: no credentials: %v", self.bucket, err)
		}
	}

	// Checksums are only for the requests and responses which need them, as
	// a service which is compatible with S3, such as MinIO, may not support
	// them, and a stream can't have one when it can't seek, without TLS. A
	// service which is compatible is addressed by its path, as the bucket
	// isn't a host of its domain.
	return s3.NewFromConfig(config, func(o *s3.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if self.endpoint != "" {
			o.BaseEndpoint = aws.String(self.endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// ping checks the bucket can be accessed, and returns its region, when it's
// in another, with an error
func (self *S3) ping(ctx context.Context, client *s3.Client) (string, error) {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(self.bucket)})
	if err == nil {
		return "", nil
	}

	// The request failed, without a response
	var responseErr *smithyhttp.ResponseError
	if !errors.As(err, &responseErr) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", gofiler.ErrServiceUnavailable.Withf("Bucket %q: %v", self.bucket, err)
	}

	// The bucket is in another region, or can't be accessed, or doesn't exist
	region := responseErr.Response.Header.Get(headerBucketRegion)
	switch status := responseErr.HTTPStatusCode(); {
	case region != "" && (status == http.StatusMovedPermanently || status == http.StatusBadRequest):
		return region, gofiler.ErrBadParameter.Withf("Bucket %q is in region %q", self.bucket, region)
	case (status == http.StatusForbidden || status == http.StatusUnauthorized) && self.anonymous:
		return "", gofiler.ErrForbidden.Withf("Bucket %q: access denied, anonymously", self.bucket)
	case status == http.StatusForbidden || status == http.StatusUnauthorized:
		return "", gofiler.ErrForbidden.Withf("Bucket %q: access denied, with the credentials", self.bucket)
	case status == http.StatusNotFound:
		return "", gofiler.ErrNotFound.Withf("Bucket %q not found", self.bucket)
	default:
		return "", gofiler.ErrServiceUnavailable.Withf("Bucket %q: %s", self.bucket, http.StatusText(status))
	}
}
