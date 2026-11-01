package fs

import (
	"net/url"
	"regexp"
	"slices"
	"strconv"

	// Packages
	filer "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// opt is the options of a filesystem, which are parsed from the query
// parameters of its URL, or from the command line
type opt struct {
	skip     []*regexp.Regexp // the names of files and directories which aren't listed
	packages []*regexp.Regexp // the names of directories which are packages
	readonly bool             // objects can't be created or deleted
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// optSkip is a regular expression of the names of files and directories
	// which aren't listed, nor anything in a directory with the name, which
	// can be repeated. When it's set, it replaces the default expressions
	// (see defaultSkip). An empty expression is ignored, so "skip=" lists
	// every name.
	optSkip = "skip"

	// optPackage is a regular expression of the names of directories which
	// are packages, which are listed as files, which can't be read, without
	// the files in them, which can be repeated. When it's set, it replaces
	// the default expressions (see defaultPackage). An empty expression is
	// ignored, so "package=" has no packages.
	optPackage = "package"

	// optReadonly is whether objects can only be read, and not created or
	// deleted, even when the filesystem is writable. It's true, false, or
	// empty, which is true. A filesystem which isn't writable is read-only
	// whatever it's set to.
	optReadonly = "readonly"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// parseOpts returns the options, from the values, which are checked: an
// unknown option, or an expression which doesn't compile, is an error
func parseOpts(values url.Values) (opt, error) {
	var self opt

	// Check the options are known
	for key := range values {
		switch key {
		case optSkip, optPackage, optReadonly:
		default:
			return opt{}, filer.ErrBadParameter.Withf("Unknown option: %q", key)
		}
	}

	// Set the names which aren't listed, and of the packages, which are the
	// defaults when the options aren't set
	if skip, err := compileExprs(values, optSkip, defaultSkip); err != nil {
		return opt{}, err
	} else {
		self.skip = skip
	}
	if packages, err := compileExprs(values, optPackage, defaultPackage); err != nil {
		return opt{}, err
	} else {
		self.packages = packages
	}

	// Set whether objects can only be read
	if values.Has(optReadonly) {
		if value := values.Get(optReadonly); value == "" {
			self.readonly = true
		} else if readonly, err := strconv.ParseBool(value); err != nil {
			return opt{}, filer.ErrBadParameter.Withf("Invalid %s option: %q", optReadonly, value)
		} else {
			self.readonly = readonly
		}
	}

	// Return success
	return self, nil
}

// values returns the options which aren't the defaults, as query
// parameters, so the filesystem can be created again with them
func (self opt) values() url.Values {
	values := url.Values{}
	setExprs(values, optSkip, self.skip, defaultSkip)
	setExprs(values, optPackage, self.packages, defaultPackage)
	if self.readonly {
		values.Set(optReadonly, "true")
	}
	return values
}

// compileExprs compiles the regular expressions of the option, or the
// defaults when it isn't set, ignoring those which are empty, as they'd
// match every name
func compileExprs(values url.Values, key string, defaults []string) ([]*regexp.Regexp, error) {
	exprs := defaults
	if values.Has(key) {
		exprs = values[key]
	}
	result := make([]*regexp.Regexp, 0, len(exprs))
	for _, expr := range exprs {
		if expr == "" {
			continue
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, filer.ErrBadParameter.Withf("Invalid %s option: %v", key, err)
		}
		result = append(result, re)
	}
	return result, nil
}

// setExprs sets the expressions of the option, when they aren't the
// defaults, or an empty expression when there are none
func setExprs(values url.Values, key string, res []*regexp.Regexp, defaults []string) {
	exprs := make([]string, 0, len(res))
	for _, re := range res {
		exprs = append(exprs, re.String())
	}
	if len(exprs) == 0 {
		values[key] = []string{""}
	} else if !slices.Equal(exprs, defaults) {
		values[key] = exprs
	}
}
