// Package s3 is a filer of the objects in an S3 bucket, or one which is
// compatible, beneath a prefix
package s3

import (
	"context"
	"net"
	"net/url"
	"regexp"
	"strings"

	// Packages
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// S3 is a filer of the objects in a bucket, beneath a prefix
type S3 struct {
	opt
	gofiler.Opts
	bucket string     // the bucket
	prefix string     // the prefix of the objects' keys, which is empty, or ends with "/"
	client *s3.Client // the client of the bucket, in its region
}

var _ gofiler.Filer = (*S3)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// bucketName is the pattern of a bucket's name: 3 to 63 lowercase
	// letters, numbers, dots and hyphens, which starts and ends with a letter
	// or number
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

	// bucketPrefixes and bucketSuffixes are those which S3 reserves, so a
	// bucket's name can't have them
	bucketPrefixes = []string{"xn--", "sthree-", "amzn-s3-demo-"}
	bucketSuffixes = []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"}
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns a filer of the objects in the bucket of the URL, which is
// s3://<bucket>/<prefix>, beneath the prefix, with the options of its query
// (see opt.go). The volume's name is the bucket's, or the name option. The
// bucket is checked, with the credentials, which are the options', whose
// placeholders the provider of credentials expands, or the profile's, or the
// default ones, such as from the environment, or none, when it's anonymous,
// which is ErrForbidden when they're missing, or can't access it, and
// ErrNotFound when it doesn't exist. When the bucket is in another region,
// and the region isn't set, it's the bucket's. The requests are traced
// with the tracer, when it's set.
func New(ctx context.Context, u *url.URL, opts gofiler.Opts) (*S3, error) {
	self, err := parse(u)
	if err != nil {
		return nil, err
	} else {
		self.Opts = opts
	}

	// Credentials which aren't placeholders are stored, so there must be a
	// provider of credentials, which stores them once the bucket is checked
	if self.Credentials == nil && ((self.keyID != "" && !isPlaceholder(self.keyID)) || (self.secret != "" && !isPlaceholder(self.secret)) || (self.token != "" && !isPlaceholder(self.token))) {
		return nil, gofiler.ErrBadParameter.Withf("Bucket %q: the credentials can't be stored, without a provider of credentials", self.bucket)
	}
	if err := self.connect(ctx); err != nil {
		return nil, err
	} else if err := self.storeCredentials(ctx); err != nil {
		return nil, err
	}
	return self, nil
}

// parse returns the filer of the URL, without a client
func parse(u *url.URL) (*S3, error) {
	self := new(S3)
	if u == nil || u.Scheme != schema.SchemeS3 {
		return nil, gofiler.ErrBadParameter.Withf("Not an %s url: %v", schema.SchemeS3, u)
	} else if !isBucketName(u.Host) {
		return nil, gofiler.ErrBadParameter.Withf("Invalid bucket: %q", u.Host)
	} else {
		self.bucket = u.Host
	}

	// The prefix, which ends with "/", so it's a directory
	if prefix := strings.Trim(u.Path, "/"); prefix != "" {
		self.prefix = prefix + "/"
	}

	// Set the options
	if values, err := url.ParseQuery(u.RawQuery); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("Invalid options: %v", err)
	} else if opt, err := parseOpts(self.bucket, values); err != nil {
		return nil, err
	} else {
		self.opt = opt
	}

	// Return success
	return self, nil
}

// Close the filer
func (self *S3) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// URL returns the URL of the bucket, which the filer can be created again
// from, with the options which are set as its query
func (self *S3) URL() *url.URL {
	return &url.URL{Scheme: schema.SchemeS3, Host: self.bucket, Path: "/" + self.prefix, RawQuery: self.opt.values(self.bucket).Encode()}
}

// Readonly returns true when objects can't be created or deleted, which is
// when requests are anonymous, without credentials, too
func (self *S3) Readonly() bool {
	return self.readonly || self.anonymous
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECTS

// CreateObject creates an object in the bucket
func (self *S3) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented.With("CreateObject")
}

// GetObject returns the metadata of an object in the bucket
func (self *S3) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented.With("GetObject")
}

// ReadObject returns an object in the bucket, and a function which reads it
func (self *S3) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	return nil, nil, gofiler.ErrNotImplemented.With("ReadObject")
}

// DeleteObjects deletes an object, or the objects beneath a directory, in the
// bucket
func (self *S3) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrNotImplemented.With("DeleteObjects")
}

// WatchObjects can't watch a bucket, which needs its event notifications, and
// a queue
func (self *S3) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("a bucket can't be watched")
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// isBucketName returns true when the name is one which S3 allows for a
// bucket: of the pattern, without adjacent dots, which isn't an IP address,
// and without the prefixes and suffixes which are reserved
func isBucketName(name string) bool {
	if !bucketName.MatchString(name) || strings.Contains(name, "..") {
		return false
	} else if ip := net.ParseIP(name); ip != nil {
		return false
	}
	for _, prefix := range bucketPrefixes {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	for _, suffix := range bucketSuffixes {
		if strings.HasSuffix(name, suffix) {
			return false
		}
	}
	return true
}
