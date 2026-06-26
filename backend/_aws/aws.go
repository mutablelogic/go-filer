package aws

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	gofiler "github.com/mutablelogic/go-filer"
	backendpkg "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pkgtypes "github.com/mutablelogic/go-server/pkg/types"
	otelaws "go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type backend struct {
	*opt
	client       *s3.Client
	bucket       string // S3 bucket name (URL host)
	bucketPrefix string // optional key prefix within the bucket (URL path)
}

var _ backendpkg.Backend = (*backend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a direct AWS SDK v2 S3 backend. Only s3:// URLs are supported.
// Examples:
//   - "s3://my-bucket"
//   - "s3://my-bucket/prefix"
func New(ctx context.Context, u string, opts ...Opt) (*backend, error) {
	self := new(backend)

	parsed, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "s3" {
		return nil, fmt.Errorf("unsupported scheme %q: aws backend only supports s3://", parsed.Scheme)
	}
	if !pkgtypes.IsIdentifier(parsed.Host) {
		return nil, fmt.Errorf("backend name %q must be a valid identifier (letter, digits, underscores, hyphens; max 64 chars)", parsed.Host)
	}

	o, err := apply(parsed, opts...)
	if err != nil {
		return nil, err
	}
	self.opt = o
	self.bucket = parsed.Host
	self.bucketPrefix = strings.TrimPrefix(strings.TrimSuffix(parsed.Path, "/"), "/")

	// Build AWS config: use provided config or load from default chain.
	cfg := o.awsConfig
	if cfg == nil {
		loaded, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		cfg = &loaded
	}

	// Apply anonymous credentials.
	if o.anonymous {
		cfgCopy := *cfg
		cfgCopy.Credentials = awssdk.AnonymousCredentials{}
		cfg = &cfgCopy
	}

	// Inject OTel middleware when a tracer is configured.
	if o.tracer != nil {
		cfgCopy := *cfg
		otelaws.AppendMiddlewares(&cfgCopy.APIOptions)
		cfg = &cfgCopy
	}

	// Build S3 client options.
	var s3Opts []func(*s3.Options)
	if o.endpoint != "" {
		ep := o.endpoint
		s3Opts = append(s3Opts, func(opt *s3.Options) {
			opt.BaseEndpoint = awssdk.String(ep)
			opt.UsePathStyle = true
		})
	}

	self.client = s3.NewFromConfig(*cfg, s3Opts...)
	return self, nil
}

// Close releases resources held by the backend.
func (b *backend) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Name returns the S3 bucket name.
func (b *backend) Name() string {
	return b.opt.url.Host
}

// URL returns the backend URL with non-credential query parameters.
func (b *backend) URL() *url.URL {
	u := &url.URL{
		Scheme: "s3",
		Host:   b.opt.url.Host,
		Path:   b.opt.url.Path,
	}
	q := url.Values{}
	if b.opt.awsConfig != nil && b.opt.awsConfig.Region != "" {
		q.Set("region", b.opt.awsConfig.Region)
	}
	if b.opt.endpoint != "" {
		if ep, err := url.Parse(b.opt.endpoint); err == nil {
			ep.User = nil
			ep.RawQuery = ""
			ep.Fragment = ""
			q.Set("endpoint", ep.String())
		}
	}
	if b.opt.anonymous {
		q.Set("anonymous", "true")
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	return u
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// key converts a logical request path to a storage key.
func (b *backend) key(p string) string {
	sk := strings.TrimPrefix(cleanPath(p), "/")
	if b.bucketPrefix != "" {
		if sk == "" {
			return b.bucketPrefix + "/"
		}
		return b.bucketPrefix + "/" + sk
	}
	return sk
}

// pathFromStorageKey converts a storage key back to a logical path.
func (b *backend) pathFromStorageKey(sk string) string {
	if b.bucketPrefix != "" {
		sk = strings.TrimPrefix(sk, b.bucketPrefix+"/")
	}
	return strings.TrimPrefix(path.Clean("/"+sk), "/")
}

// cleanPath normalises a request path.
func cleanPath(p string) string {
	if p == "" {
		p = "/"
	}
	return path.Clean(p)
}

// attrsFromHead builds an Object from a HeadObject response.
func (b *backend) attrsFromHead(objPath string, out *s3.HeadObjectOutput) *schema.Object {
	obj := &schema.Object{
		Volume:      b.Name(),
		Path:        objPath,
		ContentType: awssdk.ToString(out.ContentType),
		Size:        awssdk.ToInt64(out.ContentLength),
	}
	if out.LastModified != nil {
		obj.ModTime = *out.LastModified
	}
	if out.ETag != nil {
		obj.ETag = normaliseETag(*out.ETag)
	}
	if len(out.Metadata) > 0 {
		meta := make(schema.ObjectMeta, len(out.Metadata))
		for k, v := range out.Metadata {
			meta[k] = v
		}
		obj.Meta = meta
		if lm, ok := out.Metadata[schema.AttrLastModified]; ok {
			if t, err := time.Parse(time.RFC3339, lm); err == nil {
				obj.ModTime = t
			}
		}
	}
	return obj
}

// attrsFromListItem builds an Object from a ListObjectsV2 result item.
func (b *backend) attrsFromListItem(item s3types.Object) schema.Object {
	obj := schema.Object{
		Volume: b.Name(),
		Path:   b.pathFromStorageKey(awssdk.ToString(item.Key)),
		Size:   awssdk.ToInt64(item.Size),
	}
	if item.LastModified != nil {
		obj.ModTime = *item.LastModified
	}
	if item.ETag != nil {
		obj.ETag = normaliseETag(*item.ETag)
	}
	return obj
}

// normaliseETag ensures the ETag is in RFC 7232 double-quoted format.
func normaliseETag(etag string) string {
	if etag == "" {
		return ""
	}
	if strings.HasPrefix(etag, `"`) || strings.HasPrefix(etag, "W/") {
		return etag
	}
	return `"` + etag + `"`
}

// isNotFound returns true when err represents a missing S3 object (404).
// HeadObject returns a raw HTTP 404 rather than a typed NoSuchKey error.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	// HeadObject (and some other ops) return HTTP 404 without a typed body.
	type httpCoder interface{ HTTPStatusCode() int }
	var he httpCoder
	if errors.As(err, &he) {
		return he.HTTPStatusCode() == 404
	}
	return false
}

// s3Err maps an AWS SDK error onto a gofiler sentinel error.
func s3Err(err error, ref string) error {
	if err == nil {
		return nil
	}
	if isNotFound(err) {
		return gofiler.ErrNotFound.Withf("object %q not found", ref)
	}
	type httpCoder interface{ HTTPStatusCode() int }
	var he httpCoder
	if errors.As(err, &he) {
		switch he.HTTPStatusCode() {
		case 403:
			return gofiler.ErrForbidden.Withf("permission denied for %q", ref)
		case 412:
			return gofiler.ErrConflict.Withf("precondition failed for %q: %v", ref, err)
		}
	}
	return gofiler.ErrInternalServerError.Withf("s3 operation failed: %v", err)
}

// addSpanAttrs annotates the current span if one is active.
func addSpanAttrs(ctx context.Context, attrs ...attribute.KeyValue) {
	if span := trace.SpanFromContext(ctx); span != nil {
		span.SetAttributes(attrs...)
	}
}
