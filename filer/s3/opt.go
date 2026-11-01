package s3

import (
	"net/url"
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
	// credentials, when the bucket is checked, so the URL doesn't have them.
	// The secret access key and session token must have placeholders, and
	// the session token is optional.
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
)

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
		case optName, optRegion, optProfile, optAccessKeyID, optSecretAccessKey, optSessionToken, optEndpoint, optAnonymous, optReadonly:
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
	for key, value := range map[string]string{optSecretAccessKey: self.secret, optSessionToken: self.token} {
		if value != "" && !strings.Contains(value, "$") {
			return opt{}, filer.ErrBadParameter.Withf("The %s option must be a placeholder, such as ${S3_SECRET}, so the url doesn't have it", key)
		}
	}
	if self.keyID != "" && values.Has(optProfile) {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyID, optProfile)
	} else if self.keyID != "" && values.Has(optAnonymous) {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyID, optAnonymous)
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
// bucket's
func (self opt) values(bucket string) url.Values {
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
	if self.keyID != "" {
		values.Set(optAccessKeyID, self.keyID)
		values.Set(optSecretAccessKey, self.secret)
	}
	if self.token != "" {
		values.Set(optSessionToken, self.token)
	}
	if self.endpoint != "" {
		values.Set(optEndpoint, self.endpoint)
	}
	if self.anonymous {
		values.Set(optAnonymous, "true")
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
