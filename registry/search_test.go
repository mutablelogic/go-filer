package registry

import (
	"context"
	"slices"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Registry_Search(t *testing.T) {
	r, _, dir := newIndexedRegistry(t)
	ctx := context.Background()
	for _, name := range []string{"media", "photos"} {
		_, err := add(t, r, name, indexedURL(t, dir, name, "folders"))
		require.NoError(t, err)
		ready(t, r, name)
	}
	found := func(text string, page schema.OffsetLimit) ([]string, *schema.SearchResponse) {
		t.Helper()
		resp, err := r.Search(ctx, text, page)
		require.NoError(t, err)
		var result []string
		for _, object := range resp.Body {
			result = append(result, object.Name+":"+store.Key(&object.Object))
		}
		slices.Sort(result)
		return result, resp
	}

	// The objects of the volumes which are mounted match, of the text of the
	// query, of a page, whose limit is at most schema.SearchLimit
	got, resp := found("b", schema.OffsetLimit{})
	assert.Equal(t, []string{"media:/a/b/", "photos:/a/b/"}, got)
	assert.Equal(t, uint64(2), resp.Count)
	assert.Equal(t, []string{"b"}, resp.Query.Terms)
	assert.Equal(t, uint64(schema.SearchLimit), *resp.Limit)
	limit := uint64(1000)
	_, resp = found("b", schema.OffsetLimit{Limit: &limit})
	assert.Equal(t, uint64(schema.SearchLimit), *resp.Limit, "it's capped")

	// But not of a volume which isn't mounted, as it's disabled
	photos, err := r.GetVolume(ctx, "photos")
	require.NoError(t, err)
	_, err = r.UpdateVolume(ctx, schema.VolumeMeta{Name: "photos", URL: photos.URL, Enabled: false})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !slices.Contains(r.Keys(), "photos") }, 10*time.Second, time.Millisecond)
	got, resp = found("b", schema.OffsetLimit{})
	assert.Equal(t, []string{"media:/a/b/"}, got)
	assert.Equal(t, uint64(1), resp.Count)

	// Text which isn't a query is bad
	_, err = r.Search(ctx, `"not ended`, schema.OffsetLimit{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}
