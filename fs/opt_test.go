package fs

import (
	"net/url"
	"testing"
)

func Test_opt(t *testing.T) {
	if _, err := parseOpts(url.Values{"other": {"x"}}); err == nil {
		t.Error("unknown option")
	}
	if _, err := parseOpts(url.Values{"skip": {"("}}); err == nil {
		t.Error("bad expression")
	}
	o, err := parseOpts(url.Values{"skip": {""}})
	if err != nil || len(o.skip) != 0 || (&FS{opt: o}).skip(".git") {
		t.Error("empty skip", err)
	}
	if v := o.values().Encode(); v != "skip=" {
		t.Error(v)
	}
	o, _ = parseOpts(url.Values{"skip": {`\.tmp$`}})
	if !(&FS{opt: o}).skip("a.tmp") || (&FS{opt: o}).skip(".git") {
		t.Error("custom skip")
	}
	o, _ = parseOpts(nil)
	if v := o.values().Encode(); v != "" {
		t.Error(v)
	}
}

func Test_optDefault(t *testing.T) {
	// The default expressions, when they're set, are the defaults, so they
	// aren't in the values
	o, err := parseOpts(url.Values{"skip": defaultSkip})
	if err != nil {
		t.Fatal(err)
	}
	if v := o.values().Encode(); v != "" {
		t.Error(v)
	}
	o, _ = parseOpts(url.Values{"skip": {`\.tmp$`, `^#`}})
	if v := o.values()["skip"]; len(v) != 2 || v[0] != `\.tmp$` || v[1] != `^#` {
		t.Error(v)
	}
}
