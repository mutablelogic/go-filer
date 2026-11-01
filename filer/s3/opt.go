package s3

import (
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// opt is the options of a bucket, which are parsed from the query
// parameters of its URL
type opt struct {
	name      string // the volume's name
	region    string // the bucket's region, when it's set
	profile   string // the profile of the credentials, when it's set
	keyID     string // the access key ID, which can have placeholders, when it's set
	secret    string // the secret access key, which has placeholders, when it's set
	token     string // the session token, which has placeholders, when it's set
	endpoint  string // the endpoint of a service which is compatible with S3, when it's set
	anonymous bool   // requests aren't signed, so they're without credentials
	readonly  bool   // objects can't be created or deleted
	seaweedfs string // the address of a SeaweedFS filer's gRPC service, which is watched, when it's set
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// optName is the volume's name, which is an identifier. It defaults to
	// the bucket's name, so it's needed when that isn't an identifier, such
	// as when it has dots.
	optName = "name"

	// optRegion is the bucket's region, such as eu-west-1
	optRegion = "region"

	// optProfile is the profile of the credentials, and the configuration,
	// in the shared files, such as ~/.aws/credentials, in place of the
	// default one, or of AWS_PROFILE
	optProfile = "profile"

	// optAccessKeyID, optSecretAccessKey and optSessionToken are the
	// credentials, in place of the profile's, or the default ones, whose
	// placeholders, such as ${S3_SECRET}, are expanded by the provider of
	// credentials, when the bucket is checked. A value which isn't a
	// placeholder is stored by the provider, once the bucket is checked, and
	// the URL has its placeholder, so it never has the credentials. A
	// credential which starts with "$" is given with "$$", so $$word is
	// $word, rather than the placeholder of word. The session token is
	// optional.
	optAccessKeyID     = "access-key-id"
	optSecretAccessKey = "secret-access-key"
	optSessionToken    = "session-token"

	// optEndpoint is the http or https URL of a service which is compatible
	// with S3, such as http://localhost:9000 for MinIO
	optEndpoint = "endpoint"

	// optAnonymous is whether requests aren't signed, so they're without
	// credentials, which is read-only. It's true, false, or empty, which is
	// true.
	optAnonymous = "anonymous"

	// optReadonly is whether objects can only be read, and not created or
	// deleted. It's true, false, or empty, which is true.
	optReadonly = "readonly"

	// optSeaweedFS is the address of the gRPC service of the SeaweedFS filer
	// whose S3 gateway the endpoint is, as host:port, such as
	// 192.168.86.21:18888, which is the filer's port, plus 10000, whose
	// changes to the bucket's objects are watched
	optSeaweedFS = "seaweedfs-filer-grpc"
)

// placeholder is the pattern of a value which is only a placeholder
var placeholder = regexp.MustCompile(`^\$(?:[A-Za-z_][A-Za-z0-9_]*|\{[A-Za-z_][A-Za-z0-9_]*\})$`)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// parseOpts returns the options of the bucket, from the values, which are
// checked: an unknown option, or one which is invalid, is an error, as is a
// bucket's name which isn't an identifier, without the name option
func parseOpts(bucket string, values url.Values) (opt, error) {
	var self opt

	// Check the options are known
	for key := range values {
		switch key {
		case optName, optRegion, optProfile, optAccessKeyID, optSecretAccessKey, optSessionToken, optEndpoint, optAnonymous, optReadonly, optSeaweedFS:
		default:
			return opt{}, filer.ErrBadParameter.Withf("Unknown option: %q", key)
		}
	}

	// Set the volume's name, which is the bucket's, unless it's set
	if name := values.Get(optName); name == "" && !types.IsIdentifier(bucket) {
		return opt{}, filer.ErrBadParameter.Withf("The bucket's name %q isn't a volume's name, so the volume needs one, with the %s option", bucket, optName)
	} else if name == "" {
		self.name = bucket
	} else if !types.IsIdentifier(name) {
		return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q", optName, name)
	} else {
		self.name = name
	}

	// Set the region, profile and endpoint
	self.region = values.Get(optRegion)
	self.profile = values.Get(optProfile)
	if values.Has(optEndpoint) {
		value := values.Get(optEndpoint)
		if endpoint, err := url.Parse(value); err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
			return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q", optEndpoint, value)
		}
		self.endpoint = value
	}

	// Set the credentials, which are an access key ID and secret access key,
	// with an optional session token, whose secrets are placeholders, which
	// aren't with the profile, or anonymous
	self.keyID, self.secret, self.token = values.Get(optAccessKeyID), values.Get(optSecretAccessKey), values.Get(optSessionToken)
	if (self.keyID == "") != (self.secret == "") || (self.token != "" && self.keyID == "") {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options are both needed, with the %s option", optAccessKeyID, optSecretAccessKey, optSessionToken)
	}
	if self.keyID != "" && values.Has(optProfile) {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyID, optProfile)
	} else if self.keyID != "" && values.Has(optAnonymous) {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyID, optAnonymous)
	}

	// Set the address of the SeaweedFS filer, which is host:port
	if values.Has(optSeaweedFS) {
		value := values.Get(optSeaweedFS)
		if host, port, err := net.SplitHostPort(value); err != nil || host == "" || port == "" {
			return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q, which is host:port", optSeaweedFS, value)
		}
		self.seaweedfs = value
	}

	// Set whether requests are anonymous, and whether objects can only be
	// read
	if anonymous, err := parseBool(values, optAnonymous); err != nil {
		return opt{}, err
	} else {
		self.anonymous = anonymous
	}
	if readonly, err := parseBool(values, optReadonly); err != nil {
		return opt{}, err
	} else {
		self.readonly = readonly
	}

	// Return success
	return self, nil
}

// values returns the options which are set, as query parameters, so the
// bucket can be created again with them, without the name when it's the
// bucket's, and with the credentials, which are placeholders, when they're
// asked for
func (self opt) values(bucket string, credentials bool) url.Values {
	values := url.Values{}
	if self.name != bucket {
		values.Set(optName, self.name)
	}
	if self.region != "" {
		values.Set(optRegion, self.region)
	}
	if self.profile != "" {
		values.Set(optProfile, self.profile)
	}
	if self.keyID != "" && credentials {
		values.Set(optAccessKeyID, self.keyID)
		values.Set(optSecretAccessKey, self.secret)
	}
	if self.token != "" && credentials {
		values.Set(optSessionToken, self.token)
	}
	if self.endpoint != "" {
		values.Set(optEndpoint, self.endpoint)
	}
	if self.anonymous {
		values.Set(optAnonymous, "true")
	}
	if self.seaweedfs != "" {
		values.Set(optSeaweedFS, self.seaweedfs)
	}
	if self.readonly {
		values.Set(optReadonly, "true")
	}
	return values
}

// parseBool returns the option, which is true, false, or empty, which is
// true, or false when it isn't set
func parseBool(values url.Values, key string) (bool, error) {
	if !values.Has(key) {
		return false, nil
	} else if value := values.Get(key); value == "" {
		return true, nil
	} else if b, err := strconv.ParseBool(value); err != nil {
		return false, filer.ErrBadParameter.Withf("Invalid %s option: %q", key, value)
	} else {
		return b, nil
	}
}

// literal returns the credential of a value which isn't a placeholder, which
// is the value, unless it starts with "$$", which is a "$", so a credential
// which would be a placeholder, such as $word, is given as $$word
func literal(value string) string {
	if strings.HasPrefix(value, "$$") {
		return value[1:]
	}
	return value
}

// isPlaceholder returns true when the value of a credential is a
// placeholder, such as ${S3_SECRET} or $S3_SECRET, which the provider of
// credentials expands, rather than being the credential. A value which has a
// placeholder, and other text, is a credential, which is stored, so its text
// isn't kept.
func isPlaceholder(value string) bool {
	return placeholder.MatchString(value)
}
