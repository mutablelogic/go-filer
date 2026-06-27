package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	otelaws "go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type S3Backend struct {
	url    *url.URL
	client *s3.Client
	tracer trace.Tracer
}

var _ backend.Backend = (*S3Backend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, tracer trace.Tracer, decryptfn backend.DecryptCredentailFunc, url *url.URL) (*S3Backend, error) {
	self := new(S3Backend)

	// Get the S3 client, URL, and error
	client, url, err := s3Client(ctx, tracer, decryptfn, url)
	if err != nil {
		return nil, err
	} else {
		self.client = client
		self.url = url
		self.tracer = tracer
	}

	// Validate that the bucket exists and is accessible.
	// A 301 means the bucket is in a different region; detect it and reconnect.
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: types.Ptr(url.Host),
	})
	if err != nil {
		var httpErr *smithyhttp.ResponseError
		if errors.As(err, &httpErr) {
			if httpErr.HTTPStatusCode() == http.StatusMovedPermanently {
				region := httpErr.Response.Header.Get("X-Amz-Bucket-Region")
				if region == "" {
					return nil, fmt.Errorf("bucket %q is in a different region (no X-Amz-Bucket-Region header)", url.Host)
				}
				q := url.Query()
				q.Set("region", region)
				url.RawQuery = q.Encode()
				if client, url, err = s3Client(ctx, tracer, decryptfn, url); err != nil {
					return nil, err
				}
				self.client = client
				self.url = url
				return self, nil
			}
			return nil, httpresponse.Err(httpErr.HTTPStatusCode()).Withf("bucket %q", url.Host)
		}
		return nil, fmt.Errorf("failed to access bucket %q: %w", url.Host, err)
	}

	// Return success
	return self, nil
}

func (self *S3Backend) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (self *S3Backend) Name() string {
	return self.url.Host
}

func (self *S3Backend) URL() *url.URL {
	return self.url
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func s3Client(ctx context.Context, tracer trace.Tracer, decryptfn backend.DecryptCredentailFunc, s3url *url.URL) (*s3.Client, *url.URL, error) {
	if s3url == nil || s3url.Scheme != "s3" {
		return nil, nil, gofiler.ErrBadParameter.With("url with scheme 's3' is required")
	}

	// Validate the name
	name := s3url.Host
	prefix := strings.TrimPrefix(strings.TrimSuffix(s3url.Path, "/"), "/")
	if !types.IsIdentifier(name) {
		return nil, nil, gofiler.ErrBadParameter.Withf("invalid s3 backend name: %q", name)
	}

	// Parse query parameters
	q := s3url.Query()
	anonymous := q.Get("anonymous") == "true"
	endpoint := q.Get("endpoint")
	region := q.Get("region")

	// Build AWS config
	var cfgOpts []func(*awsconfig.LoadOptions) error
	if region != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithRegion(region))
	}

	// Explicit credentials take precedence over anonymous access
	var accessKey, secretKey []byte
	if accessCredential := q.Get("access-key"); accessCredential != "" {
		var err error
		accessKey, err = decryptfn(ctx, accessCredential)
		if err != nil {
			return nil, nil, err
		}
	}
	if secretCredential := q.Get("secret-key"); secretCredential != "" {
		var err error
		secretKey, err = decryptfn(ctx, secretCredential)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(accessKey) != 0 && len(secretKey) != 0 {
		accessKey, err := strconv.Unquote(string(accessKey))
		if err != nil {
			return nil, nil, gofiler.ErrBadParameter.Withf("invalid access-key: %v", err)
		}
		secretKey, err := strconv.Unquote(string(secretKey))
		if err != nil {
			return nil, nil, gofiler.ErrBadParameter.Withf("invalid secret-key: %v", err)
		}
		cfgOpts = append(cfgOpts, awsconfig.WithCredentialsProvider(
			awscredentials.NewStaticCredentialsProvider(string(accessKey), string(secretKey), ""),
		))
	} else if anonymous {
		cfgOpts = append(cfgOpts, awsconfig.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}

	config, err := awsconfig.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, nil, gofiler.ErrBadParameter.Withf("failed to load AWS config: %v", err)
	}
	if tracer != nil {
		otelaws.AppendMiddlewares(&config.APIOptions)
	}

	// Build S3 client options
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			// Suppress noisy "no supported checksum" warnings for responses from
			// servers that don't implement AWS checksum extensions (e.g. MinIO).
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		},
	}
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	// Make client, preserving non-credential query params in the canonical URL
	u := &url.URL{
		Scheme: "s3",
		Host:   name,
		Path:   "/" + prefix,
	}
	uq := url.Values{}
	if region != "" {
		uq.Set("region", region)
	}
	if endpoint != "" {
		uq.Set("endpoint", endpoint)
	}
	if anonymous {
		uq.Set("anonymous", "true")
	}
	if accessKey := q.Get("access-key"); accessKey != "" {
		uq.Set("access-key", accessKey)
	}
	if secretKey := q.Get("secret-key"); secretKey != "" {
		uq.Set("secret-key", secretKey)
	}
	if len(uq) > 0 {
		u.RawQuery = uq.Encode()
	}
	return s3.NewFromConfig(config, s3Opts...), u, nil
}

////////////////////////////////////////////////////////////////////////////////
// STUBS

// Create object in the backend
func (self *S3Backend) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented.With("CreateObject")
}

// Delete objects in the backend (single object or prefix)
func (self *S3Backend) DeleteObjects(context.Context, schema.DeleteObjectsRequest) error {
	return gofiler.ErrNotImplemented.With("DeleteObjects")
}
