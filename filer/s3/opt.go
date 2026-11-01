package s3

import (
	"net/url"
	"strconv"

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
	endpoint  string // the endpoint of a service which is compatible with S3, when it's set
	anonymous bool   // requests aren't signed, so they're without credentials
	readonly  bool   // objects can't be created or deleted
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// optName is the volume's name, which is an identifier, and which is
	// needed, as a bucket's name can't be one
	optName = "name"

	// optRegion is the bucket's region, such as eu-west-1
	optRegion = "region"

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

// parseOpts returns the options, from the values, which are checked: an
// unknown option, or one which is invalid, is an error, and the name is
// needed
func parseOpts(values url.Values) (opt, error) {
	var self opt

	// Check the options are known
	for key := range values {
		switch key {
		case optName, optRegion, optEndpoint, optAnonymous, optReadonly:
		default:
			return opt{}, filer.ErrBadParameter.Withf("Unknown option: %q", key)
		}
	}

	// Set the volume's name
	if name := values.Get(optName); name == "" {
		return opt{}, filer.ErrBadParameter.Withf("The volume needs a name, with the %s option", optName)
	} else if !types.IsIdentifier(name) {
		return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q", optName, name)
	} else {
		self.name = name
	}

	// Set the region and endpoint
	self.region = values.Get(optRegion)
	if values.Has(optEndpoint) {
		value := values.Get(optEndpoint)
		if endpoint, err := url.Parse(value); err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
			return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q", optEndpoint, value)
		}
		self.endpoint = value
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
// bucket can be created again with them
func (self opt) values() url.Values {
	values := url.Values{optName: {self.name}}
	if self.region != "" {
		values.Set(optRegion, self.region)
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
