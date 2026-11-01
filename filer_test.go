package filer_test

import (
	"net/url"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
)

func Test_PatchValues(t *testing.T) {
	values := url.Values{"a": {"1"}, "b": {"2", "3"}, "c": {"4"}}

	// Each key of the patch replaces the option's values, or removes it, and
	// neither is changed
	result := gofiler.PatchValues(values, url.Values{"b": {"5"}, "c": nil, "d": {"6", "7"}})
	assert.Equal(t, url.Values{"a": {"1"}, "b": {"5"}, "d": {"6", "7"}}, result)
	assert.Equal(t, url.Values{"a": {"1"}, "b": {"2", "3"}, "c": {"4"}}, values)
	result["a"][0] = "changed"
	assert.Equal(t, "1", values["a"][0])
	assert.Equal(t, values, gofiler.PatchValues(values, nil))
}
