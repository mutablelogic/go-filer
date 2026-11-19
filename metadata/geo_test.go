package metadata

import (
	"testing"

	// Packages
	gometadata "github.com/mutablelogic/go-metadata"
	geo "github.com/mutablelogic/go-metadata/pkg/geo"
	assert "github.com/stretchr/testify/assert"
)

func Test_shapes(t *testing.T) {
	alt := 30.0
	position := geo.LatLong{Lat: 51.5, Long: -0.12, Alt: &alt}
	square := geo.NewPolygon([]geo.Position{{Long: 0, Lat: 0}, {Long: 2, Lat: 0}, {Long: 2, Lat: 1}, {Long: 0, Lat: 1}, {Long: 0, Lat: 0}})
	hint := func(hints ...string) gometadata.Metadata {
		return entry{gometadata.MetadataNamespace, gometadata.HintKey, hints}
	}

	// The positions, and geometries, of a file with the geo hint, of any
	// namespace and key, are its shapes, but not those which aren't valid,
	// or other values
	entries := []gometadata.Metadata{
		hint(gometadata.HintMarkdown, gometadata.HintGeo),
		entry{"exif", "GPSPosition", position},
		entry{"quicktime", "location", &geo.LatLong{Lat: 52.47, Long: 13.34}},
		entry{"geo", "region", square},
		entry{"geo", "bad", geo.LatLong{Lat: 91}},
		entry{"geo", "none", (*geo.LatLong)(nil)},
		entry{"dc", "title", "A photo"},
	}
	assert.Equal(t, []geo.Geometry{position.Geometry(), geo.NewPoint(52.47, 13.34), square}, shapes(entries))

	// Without the hint, it has none
	assert.Nil(t, shapes(entries[1:]))
	assert.Nil(t, shapes(append([]gometadata.Metadata{hint(gometadata.HintMarkdown)}, entries[1:]...)))
}
