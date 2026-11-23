package metadata

import (
	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	gometadata "github.com/mutablelogic/go-metadata"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// artwork returns the artwork of go-metadata's entries, of its artwork
// namespace, such as artwork:cover, or artwork:thumbnail-2, whose key is its
// context: of its type, which go-metadata detected, its size, of its image,
// which go-metadata decoded, and its data, which go-metadata downscaled, and
// encoded, without those which aren't valid, or whose context is another's
func artwork(entries []gometadata.Metadata) []schema.ArtworkMeta {
	var result []schema.ArtworkMeta
	contexts := make(map[string]bool)
	for _, entry := range entries {
		if entry.NS() != gometadata.ArtworkNamespace || contexts[entry.Key()] {
			continue
		}
		img := entry.Image()
		if img == nil {
			continue
		}
		bounds := img.Bounds()
		a := schema.ArtworkMeta{Type: entry.Value(), Width: uint(bounds.Dx()), Height: uint(bounds.Dy()), Context: entry.Key(), Data: entry.Bytes()}
		if store.ValidateArtwork(a) != nil {
			continue
		}
		contexts[a.Context] = true
		result = append(result, a)
	}
	return result
}
