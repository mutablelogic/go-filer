package client

import (
	"net/url"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	url.Values
	fn ProgressFunc
}

// An Option to set on the client
type Opt func(*opt) error

// Uploader progress function
type ProgressFunc func(cur, total uint64)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func applyOpts(opts ...Opt) (*opt, error) {
	o := new(opt)
	o.Values = make(url.Values)
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// Set prefix for listing objects
func WithPrefix(v *string) Opt {
	return OptSet("prefix", types.PtrString(v))
}

// Set progress function for uploader
func WithProgress(fn ProgressFunc) Opt {
	return func(o *opt) error {
		o.fn = fn
		return nil
	}
}

func OptSet(k, v string) Opt {
	return func(o *opt) error {
		if v == "" {
			o.Del(k)
		} else {
			o.Set(k, v)
		}
		return nil
	}
}
