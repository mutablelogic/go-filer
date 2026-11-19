package metadata

import (
	"slices"

	// Packages
	gometadata "github.com/mutablelogic/go-metadata"
	geo "github.com/mutablelogic/go-metadata/pkg/geo"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// shapes returns the shapes of where a file is, of go-metadata's positions,
// such as a photo's exif:GPSPosition, which are points, and its geometries,
// of whatever namespace and key they're of, when its hints say it has
// places (gometadata.HintGeo), and none otherwise. Those which aren't
// valid are skipped.
func shapes(entries []gometadata.Metadata) []geo.Geometry {
	if !hinted(entries, gometadata.HintGeo) {
		return nil
	}
	var result []geo.Geometry
	for _, entry := range entries {
		var shape geo.Geometry
		switch v := entry.Any().(type) {
		case geo.LatLong:
			shape = v.Geometry()
		case *geo.LatLong:
			if v == nil {
				continue
			}
			shape = v.Geometry()
		case geo.Geometry:
			shape = v
		case *geo.Geometry:
			if v == nil {
				continue
			}
			shape = *v
		default:
			continue
		}
		if shape.Validate() == nil {
			result = append(result, shape)
		}
	}
	return result
}

// hinted returns true when the entries' hints, of metadata:hint, have the
// hint
func hinted(entries []gometadata.Metadata, hint string) bool {
	for _, entry := range entries {
		if entry.NS() != gometadata.MetadataNamespace || entry.Key() != gometadata.HintKey {
			continue
		} else if hints, ok := entry.Any().([]string); ok && slices.Contains(hints, hint) {
			return true
		}
	}
	return false
}
