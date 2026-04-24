package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"syscall"
	"time"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	backendpkg "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	otelaws "go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
	blob "gocloud.dev/blob"
	s3blob "gocloud.dev/blob/s3blob"
	gcerrors "gocloud.dev/gcerrors"

	// Drivers
	_ "gocloud.dev/blob/fileblob" // file:// URLs
	_ "gocloud.dev/blob/memblob"  // mem:// URLs
	_ "gocloud.dev/blob/s3blob"   // s3:// URLs
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type backend struct {
	*opt
	bucket       *blob.Bucket
	bucketPrefix string // key prefix for bucket operations (empty for file://)
	s3Client     *s3svc.Client
	s3Bucket     string
}

var _ backendpkg.Backend = (*backend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewBlobBackend creates a new blob backend using Go CDK.
// Supported URL schemes: s3://, file://, mem://
// Examples:
//   - "s3://my-bucket?region=us-east-1"
//   - "file:///path/to/directory"
//   - "mem://"
//
// For S3 URLs, you can optionally provide an aws.Config via WithAWSConfig()
// for full control over AWS SDK configuration.
func New(ctx context.Context, u string, opts ...Opt) (*backend, error) {
	self := new(backend)

	// Set the options
	if url, err := url.Parse(u); err != nil {
		return nil, err
	} else if opt, err := apply(url, opts...); err != nil {
		return nil, err
	} else {
		self.opt = opt
	}

	// Reject unsupported schemes up front
	switch self.url.Scheme {
	case "s3", "file", "mem":
		// supported
	default:
		return nil, fmt.Errorf("unsupported backend scheme %q: supported schemes are s3://, file://, mem://", self.url.Scheme)
	}

	// Validate the backend name (URL host) is a valid identifier
	if !types.IsIdentifier(self.url.Host) {
		return nil, fmt.Errorf("backend name %q must be a valid identifier (letter, digits, underscores, hyphens; max 64 chars)", self.url.Host)
	}

	// For s3/mem: bucketPrefix is prepended to paths to form storage keys
	// (bucket opens at host level). For file://: no prefix needed.
	if self.url.Scheme != "file" {
		self.bucketPrefix = strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	}
	if self.url.Scheme == "s3" {
		self.s3Bucket = self.url.Host
	}

	// Open the bucket
	var bucket *blob.Bucket
	var err error
	if self.url.Scheme == "s3" && self.awsConfig != nil {
		// Use the provided AWS config to open S3 bucket directly.
		// Honour WithEndpoint and WithAnonymous even though they bypass URL query parameters.
		cfg := *self.awsConfig
		if self.anonymous {
			cfg.Credentials = aws.AnonymousCredentials{}
		}
		var s3Opts []func(*s3svc.Options)
		if self.endpoint != "" {
			epURL := self.endpoint
			s3Opts = append(s3Opts, func(o *s3svc.Options) {
				o.BaseEndpoint = aws.String(epURL)
				o.UsePathStyle = true
			})
		}
		// Inject OTel instrumentation so each S3 API call produces a child span,
		// but only when a tracer is configured to avoid overhead in non-tracing deployments.
		if self.tracer != nil {
			otelaws.AppendMiddlewares(&cfg.APIOptions)
		}
		client := s3svc.NewFromConfig(cfg, s3Opts...)
		self.s3Client = client
		bucket, err = s3blob.OpenBucket(ctx, client, self.url.Host, nil)
	} else if self.url.Scheme == "file" {
		// For file:// the path is the bucket root dir - open using just the path.
		// Only forward fileblob-recognised query params; S3 params (endpoint,
		// disable_https, etc.) are stored on the shared URL but must not reach fileblob.
		// Temp files are written to os.TempDir() (TMPDIR env var), which should be
		// on the same filesystem as the data dir to avoid cross-device link errors.
		fileblobParams := url.Values{}
		for _, key := range []string{"create_dir", "no_tmp_dir", "dir_file_mode"} {
			if v := self.url.Query().Get(key); v != "" {
				fileblobParams.Set(key, v)
			}
		}
		openURL := &url.URL{Scheme: "file", Path: self.url.Path, RawQuery: fileblobParams.Encode()}
		bucket, err = blob.OpenBucket(ctx, openURL.String())
	} else {
		// For mem:// (and URL-based s3://): strip to root and filter out any
		// params that aren't valid for the target driver.
		openURL := *self.url
		openURL.Path = ""
		openURL.RawPath = ""
		switch self.url.Scheme {
		case "mem":
			// memblob only accepts "nomd5"
			memParams := url.Values{}
			if v := self.url.Query().Get("nomd5"); v != "" {
				memParams.Set("nomd5", v)
			}
			openURL.RawQuery = memParams.Encode()
		}
		bucket, err = blob.OpenBucket(ctx, openURL.String())
	}

	// Check for errors opening the bucket
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket: %w", err)
	}

	// Success
	self.bucket = bucket

	return self, nil
}

// NewFileBackend creates a file-based backend with a logical name.
// name must be a valid identifier (see types.IsIdentifier): starts with a
// letter, contains only letters, digits, underscores, or hyphens, max 64 chars.
// dir must be an absolute path; if it doesn't start with "/" an error is returned.
func NewFileBackend(ctx context.Context, name, dir string, opts ...Opt) (*backend, error) {
	if !path.IsAbs(dir) {
		return nil, fmt.Errorf("backend dir %q must be an absolute path", dir)
	}
	u := types.Ptr(url.URL{Scheme: "file", Host: name, Path: path.Clean(dir)})
	return New(ctx, u.String(), opts...)
}

// Close the backend
func (b *backend) Close() error {
	var result error
	if b.bucket != nil {
		result = errors.Join(result, b.bucket.Close())
		b.bucket = nil
	}

	// Return any errors
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Name returns the name of the backend (the host component of the URL)
func (b *backend) Name() string {
	return b.url.Host
}

// URL returns the backend destination URL.
// Query parameters carry useful non-credential details:
//   - region: AWS region (S3 only, when an awsConfig is present)
//   - endpoint: custom S3-compatible endpoint (when set)
//   - anonymous: "true" when anonymous credentials are used
func (b *backend) URL() *url.URL {
	u := &url.URL{
		Scheme: b.url.Scheme,
		Host:   b.url.Host,
		Path:   b.url.Path,
	}
	// Query params (region, endpoint, anonymous) are only meaningful for s3:// backends.
	if b.url.Scheme == "s3" {
		q := url.Values{}
		if b.awsConfig != nil && b.awsConfig.Region != "" {
			q.Set("region", b.awsConfig.Region)
		}
		if b.endpoint != "" {
			// Sanitize: strip userinfo, query, and fragment — only scheme+host+path is safe to expose
			if ep, err := url.Parse(b.endpoint); err == nil {
				ep.User = nil
				ep.RawQuery = ""
				ep.Fragment = ""
				q.Set("endpoint", ep.String())
			}
		}
		if b.anonymous {
			q.Set("anonymous", "true")
		}
		if len(q) > 0 {
			u.RawQuery = q.Encode()
		}
	}
	return u
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// key returns the blob storage key for a given request path.
// Cleans the path, strips the leading slash, and prepends the bucket prefix.
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

// storageKeyCandidates returns storage keys to try for object-addressed operations.
// The first key is always the canonical key already computed by key().
//
// We also try a legacy leading-slash variant because some
// S3-compatible stores (or historical writers) may have persisted keys like
// "/file" while current writers persist "file".
func (b *backend) storageKeyCandidates(primary string) []string {
	candidates := []string{primary}

	if primary == "" {
		return candidates
	}

	if b.bucketPrefix == "" {
		if !strings.HasPrefix(primary, "/") {
			candidates = append(candidates, "/"+primary)
		}
		return candidates
	}

	prefix := b.bucketPrefix + "/"
	if strings.HasPrefix(primary, prefix) {
		rel := strings.TrimPrefix(primary, prefix)
		if rel != "" {
			candidates = append(candidates, prefix+"/"+rel)
		}
	}

	return candidates
}

// cleanPath normalises a request path for use as Object.Path.
func cleanPath(p string) string {
	if p == "" {
		p = "/"
	}
	return path.Clean(p)
}

func (b *backend) attrsToObject(objPath string, attrs *blob.Attributes) *schema.Object {
	obj := &schema.Object{
		Path:        objPath,
		Size:        attrs.Size,
		ModTime:     attrs.ModTime,
		ContentType: attrs.ContentType,
	}
	// Prefer MD5-as-hex for ETag to stay consistent with the list iterator,
	// which only exposes MD5. Fall back to the raw ETag string when MD5 is absent.
	// Always normalise to RFC 7232 double-quoted format.
	if len(attrs.MD5) > 0 {
		obj.ETag = normaliseETag(fmt.Sprintf("%x", attrs.MD5))
	} else if attrs.ETag != "" {
		obj.ETag = normaliseETag(attrs.ETag)
	}
	if len(attrs.Metadata) > 0 {
		obj.Meta = attrs.Metadata
		// If the object was uploaded with an original mod time stored in custom
		// metadata, use that value instead of the storage-layer write time.
		if lm, ok := attrs.Metadata[schema.AttrLastModified]; ok {
			if t, err := time.Parse(time.RFC3339, lm); err == nil {
				obj.ModTime = t
			}
		}
	}
	return obj
}

// pathFromStorageKey converts a blob storage key back to a logical path
// by stripping the bucket prefix (for s3/mem with bucket prefix).
func (b *backend) pathFromStorageKey(sk string) string {
	if b.bucketPrefix != "" {
		sk = strings.TrimPrefix(sk, b.bucketPrefix+"/")
	}
	if !strings.HasPrefix(sk, "/") {
		sk = "/" + sk
	}
	return path.Clean(sk)
}

// isRealObject checks whether the storage key refers to a single real object
// (as opposed to a phantom directory — a size-0 pseudo-object with children).
// Returns the object's attributes if real, nil otherwise.
// Permission errors are propagated as a non-nil error instead of being swallowed.
func (b *backend) isRealObject(ctx context.Context, sk string) (*blob.Attributes, error) {
	if sk == "" || strings.HasSuffix(sk, "/") {
		return nil, nil
	}

	var attrs *blob.Attributes
	var foundKey string
	candidates := b.storageKeyCandidates(sk)
	addSpanAttrs(ctx, attribute.String("blob.probe_candidates", strings.Join(candidates, ",")))
	for _, candidate := range candidates {
		a, err := b.bucket.Attributes(ctx, candidate)
		if err == nil {
			attrs, foundKey = a, candidate
			addSpanAttrs(ctx, attribute.String("blob.probe_hit_key", candidate))
			break
		}
		// Surface permission errors rather than masking them as "not found".
		if gcerrors.Code(err) == gcerrors.PermissionDenied {
			return nil, blobErr(err, candidate)
		}
	}

	if attrs == nil {
		return nil, nil
	}

	if attrs.Size > 0 {
		return attrs, nil
	}
	// Size is 0 — check if there are objects with this key as a prefix.
	// If children exist, this is a phantom directory.
	iter := b.bucket.List(&blob.ListOptions{Prefix: foundKey + "/"})
	if _, err := iter.Next(ctx); err == io.EOF {
		return attrs, nil // no children → real (empty) object
	}
	return nil, nil // has children → phantom directory
}

// blobErr maps go-cloud blob errors onto module-level gofiler errors.
func blobErr(err error, ref string) error {
	if err == nil {
		return nil
	}
	// Check for OS-level errors before go-cloud classification, since the
	// gcerrors default path wraps with %v and breaks the chain.
	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EEXIST) {
		return gofiler.ErrBadParameter.Withf("cannot overwrite directory with file: %q", ref)
	}
	switch gcerrors.Code(err) {
	case gcerrors.NotFound:
		return gofiler.ErrNotFound.Withf("object %q not found", ref)
	case gcerrors.PermissionDenied:
		return gofiler.ErrForbidden.Withf("permission denied for %q", ref)
	case gcerrors.InvalidArgument:
		return gofiler.ErrBadParameter.Withf("invalid argument for %q: %v", ref, err)
	case gcerrors.FailedPrecondition:
		return gofiler.ErrConflict.Withf("precondition failed for %q: %v", ref, err)
	default:
		return gofiler.ErrInternalServerError.Withf("blob operation failed: %v", err)
	}
}

// addSpanAttrs annotates the current span if one is present on the context.
func addSpanAttrs(ctx context.Context, attrs ...attribute.KeyValue) {
	if span := trace.SpanFromContext(ctx); span != nil {
		span.SetAttributes(attrs...)
	}
}

// keyExistsByList checks for an exact storage key via ListObjectsV2 semantics.
func (b *backend) keyExistsByList(ctx context.Context, key string) (bool, error) {
	iter := b.bucket.List(&blob.ListOptions{Prefix: key})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, err
		}
		if obj.Key == key {
			return true, nil
		}
		if !strings.HasPrefix(obj.Key, key) {
			return false, nil
		}
	}
}

// deleteStorageKeyDirect performs an S3 DeleteObject call without a HeadObject precheck.
func (b *backend) deleteStorageKeyDirect(ctx context.Context, key string) error {
	if b.url.Scheme != "s3" || key == "" {
		return errors.New("direct s3 delete unavailable")
	}

	client := b.s3Client
	if client == nil {
		var asClient *s3svc.Client
		if b.bucket == nil || !b.bucket.As(&asClient) || asClient == nil {
			return errors.New("direct s3 delete unavailable")
		}
		client = asClient
	}

	_, err := client.DeleteObject(ctx, &s3svc.DeleteObjectInput{
		Bucket: aws.String(b.s3Bucket),
		Key:    aws.String(key),
	})
	return err
}

// normaliseETag ensures the ETag value is in the RFC 7232 double-quoted format
// (e.g. "\"abc123\""). S3 multipart ETags are returned by the SDK already quoted;
// MD5-derived ETags and some other backends return them as bare hex strings.
// This function is idempotent: already-quoted or weak (W/) values are left as-is.
func normaliseETag(etag string) string {
	if etag == "" {
		return ""
	}
	// Already a valid strong or weak ETag.
	if strings.HasPrefix(etag, `"`) || strings.HasPrefix(etag, "W/") {
		return etag
	}
	return `"` + etag + `"`
}
