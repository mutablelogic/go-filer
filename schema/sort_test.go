package schema_test

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ParseSort(t *testing.T) {
	for sort, want := range map[string]struct {
		order      string
		descending bool
	}{
		"":          {schema.SortName, false},
		"name":      {schema.SortName, false},
		"size":      {schema.SortSize, false},
		"-size":     {schema.SortSize, true},
		"title":     {schema.SortTitle, false},
		"-kind":     {schema.SortKind, true},
		" modified": {schema.SortModified, false},
		"-type":     {schema.SortType, true},
	} {
		order, descending, err := schema.ParseSort(sort)
		assert.NoError(t, err, sort)
		assert.Equal(t, want.order, order, sort)
		assert.Equal(t, want.descending, descending, sort)
	}
	for _, sort := range []string{"-", "colour", "Size", "--size"} {
		_, _, err := schema.ParseSort(sort)
		assert.Error(t, err, sort)
	}
}
