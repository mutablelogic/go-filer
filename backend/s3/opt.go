package s3

import (
	"net"
	"net/url"
	"regexp"
	"strconv"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// opt is the options of a bucket, which are parsed from the query
// parameters of its URL
type opt struct {
	name        string // the volume's name
	region      string // the bucket's region, when it's set
	profile     string // the profile of the credentials, when it's set
	keyID       string // the name of the credential of the access key ID, when it's set
	secret      string // the name of the credential of the secret access key, when it's set
	token       string // the name of the credential of the session token, when it's set
	endpoint    string // the endpoint of a service which is compatible with S3, when it's set
	anonymous   bool   // requests aren't signed, so they're without credentials
	readonly    bool   // objects can't be created or deleted
	seaweedfs   string // the address of a SeaweedFS filer's gRPC service, which is watched, when it's set
	description string // what the volume is, which is of the bucket, by default
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

	// optAccessKeyCredential, optSecretKeyCredential and
	// optSessionTokenCredential are the names of the server's credentials,
	// such as S3_KEY and S3_SECRET, of the access key ID, the secret access
	// key, and the session token, which is optional, in place of the
	// profile's, or the default ones. They're looked up, of the provider of
	// credentials, such as of its store, or the environment, each time
	// they're used, so the URL has their names, and never the credentials.
	optAccessKeyCredential    = "access-key-credential"
	optSecretKeyCredential    = "secret-key-credential"
	optSessionTokenCredential = "session-token-credential"

	// optAccessKeyID, optSecretAccessKey and optSessionToken are the
	// options of earlier versions, of placeholders of the credentials'
	// names, such as ${S3_SECRET}, which a stored volume has, and which are
	// read as their names
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

// credentialName is the name of a credential, as an environment variable's,
// and placeholder an earlier version's placeholder of one, ${NAME} or $NAME
var (
	credentialName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	placeholder    = regexp.MustCompile(`^\$(?:([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\})$`)
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
		case optName, optRegion, optProfile, optAccessKeyCredential, optSecretKeyCredential, optSessionTokenCredential, optAccessKeyID, optSecretAccessKey, optSessionToken, optEndpoint, optAnonymous, optReadonly, optSeaweedFS, schema.OptDescription:
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

	// Set the region, profile and endpoint, which aren't set when they're
	// empty, as a URL has each of them
	self.region = values.Get(optRegion)
	self.profile = values.Get(optProfile)
	if value := values.Get(optEndpoint); value != "" {
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

	// Set the names of the credentials, of an access key ID and secret
	// access key, with an optional session token, which aren't with the
	// profile, or anonymous
	for _, credential := range []struct {
		option, earlier string
		name            *string
	}{
		{optAccessKeyCredential, optAccessKeyID, &self.keyID},
		{optSecretKeyCredential, optSecretAccessKey, &self.secret},
		{optSessionTokenCredential, optSessionToken, &self.token},
	} {
		name, err := credentialOption(values, credential.option, credential.earlier)
		if err != nil {
			return opt{}, err
		}
		*credential.name = name
	}
	if (self.keyID == "") != (self.secret == "") || (self.token != "" && self.keyID == "") {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options are both needed, with the %s option", optAccessKeyCredential, optSecretKeyCredential, optSessionTokenCredential)
	}
	if self.keyID != "" && self.profile != "" {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyCredential, optProfile)
	} else if self.keyID != "" && self.anonymous {
		return opt{}, filer.ErrBadParameter.Withf("The %s and %s options can't be used together", optAccessKeyCredential, optAnonymous)
	}

	// Set the address of the SeaweedFS filer, which is host:port, unless
	// it's empty
	if value := values.Get(optSeaweedFS); value != "" {
		if host, port, err := net.SplitHostPort(value); err != nil || host == "" || port == "" {
			return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q, which is host:port", optSeaweedFS, value)
		}
		self.seaweedfs = value
	}

	// Set what the volume is, which is of the bucket, once it's checked,
	// when it isn't set
	self.description = values.Get(schema.OptDescription)

	// Return success
	return self, nil
}

// values returns each of the options, as query parameters, so the bucket
// can be created again with them, and a URL says how it's set up, with the
// names of its credentials, which aren't the credentials, so they're always
// there
func (self opt) values() url.Values {
	// Each option which isn't a credential is set, empty when it isn't, so
	// the URL says how it's set up
	values := url.Values{
		optName:      {self.name},
		optRegion:    {self.region},
		optProfile:   {self.profile},
		optEndpoint:  {self.endpoint},
		optSeaweedFS: {self.seaweedfs},
	}
	if self.keyID != "" {
		values.Set(optAccessKeyCredential, self.keyID)
		values.Set(optSecretKeyCredential, self.secret)
	}
	if self.token != "" {
		values.Set(optSessionTokenCredential, self.token)
	}
	// An anonymous bucket is read-only, whatever its readonly option
	values.Set(optAnonymous, strconv.FormatBool(self.anonymous))
	values.Set(optReadonly, strconv.FormatBool(self.readonly || self.anonymous))
	if self.description != "" {
		values.Set(schema.OptDescription, self.description)
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

// credentialOption returns the name of a credential, of its option, such as
// access-key-credential=S3_KEY, or else of its earlier option, of a
// placeholder of its name, such as access-key-id=${S3_KEY}, which a stored
// volume has, or "". A value which isn't a name, such as a credential
// itself, is ErrBadParameter, so a URL never has one.
func credentialOption(values url.Values, option, earlier string) (string, error) {
	if name := values.Get(option); name != "" {
		if !credentialName.MatchString(name) {
			return "", filer.ErrBadParameter.Withf("The %s option is the name of the server's credential, such as S3_SECRET, of letters, digits and underscores, not %q", option, name)
		}
		return name, nil
	} else if value := values.Get(earlier); value == "" {
		return "", nil
	} else if m := placeholder.FindStringSubmatch(value); m != nil {
		return m[1] + m[2], nil
	}
	return "", filer.ErrBadParameter.Withf("The %s option is a credential, rather than the name of one, so store it as the server's, such as with filer credential-set, and name it with the %s option", earlier, option)
}

// currentCredentials returns the options, whose earlier versions' options of
// credentials, of placeholders of their names, such as
// access-key-id=${S3_KEY}, are their options, such as
// access-key-credential=S3_KEY, so they replace those of a URL
func currentCredentials(values url.Values) (url.Values, error) {
	result := url.Values{}
	for key, value := range values {
		result[key] = value
	}
	for _, credential := range [][2]string{
		{optAccessKeyCredential, optAccessKeyID},
		{optSecretKeyCredential, optSecretAccessKey},
		{optSessionTokenCredential, optSessionToken},
	} {
		if !result.Has(credential[1]) {
			continue
		}
		name, err := credentialOption(result, credential[0], credential[1])
		if err != nil {
			return nil, err
		}
		result.Del(credential[1])
		result.Set(credential[0], name)
	}
	return result, nil
}
