package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	credentials "github.com/aws/aws-sdk-go-v2/credentials"
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
// region. The region is set to the client's, so it's in the URL, and the
// bucket isn't redirected again.
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
	self.region = client.Options().Region
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
	} else if self.keyID != "" {
		provider, err := self.staticCredentials(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(provider))
	}
	config, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("Bucket %q: %v", self.bucket, err)
	}
	if config.Region == "" {
		config.Region = defaultRegion
	}

	// Trace the requests
	if self.Tracer != nil {
		config.APIOptions = append(config.APIOptions, self.traceRequests(config.Region))
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

// staticCredentials returns the credentials of the options, whose
// placeholders are expanded by the provider of credentials, or which are the
// credentials, which are stored once the bucket is checked
func (self *S3) staticCredentials(ctx context.Context) (aws.CredentialsProvider, error) {
	values := make([]string, 0, 3)
	for _, value := range []string{self.keyID, self.secret, self.token} {
		if !isPlaceholder(value) {
			values = append(values, value)
		} else if self.Credentials == nil {
			return nil, gofiler.ErrBadParameter.Withf("Bucket %q: the credentials can't be expanded, without a provider of credentials", self.bucket)
		} else if expanded, err := self.Credentials.Expand(ctx, value); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, gofiler.ErrBadParameter.Withf("Bucket %q: credentials: %v", self.bucket, err)
		} else {
			values = append(values, expanded)
		}
	}
	return credentials.NewStaticCredentialsProvider(values[0], values[1], values[2]), nil
}

// storeCredentials stores the credentials of the options which aren't
// placeholders, with the provider of credentials, by keys of the volume's
// name, such as S3_PHOTOS_SECRET_ACCESS_KEY, as JSON strings, and replaces
// them with their placeholders, so the URL doesn't have them
func (self *S3) storeCredentials(ctx context.Context) error {
	for _, credential := range []struct {
		option string
		value  *string
	}{
		{optAccessKeyID, &self.keyID},
		{optSecretAccessKey, &self.secret},
		{optSessionToken, &self.token},
	} {
		if *credential.value == "" || isPlaceholder(*credential.value) {
			continue
		}
		key := credentialKey(self.name, credential.option)
		data, err := json.Marshal(*credential.value)
		if err != nil {
			return err
		} else if err := self.Credentials.Set(ctx, key, data); err != nil {
			return fmt.Errorf("Bucket %q: storing the %s option: %w", self.bucket, credential.option, err)
		}
		*credential.value = "${" + key + "}"
	}
	return nil
}

// credentialKey returns the key of a credential of a volume, which is the
// option of its URL, such as S3_PHOTOS_SECRET_ACCESS_KEY
func credentialKey(name, option string) string {
	return strings.ToUpper(strings.NewReplacer("-", "_").Replace("s3_" + name + "_" + option))
}
