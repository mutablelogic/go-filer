package client

import (
	"net/url"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	url.Values
}

type Opt func(*opt) error

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
