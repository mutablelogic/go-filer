// Package icon is the icons of the backends, which are their images, of
// their media types, such as an SVG image a backend embeds
package icon

import (
	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// icon is an image, of its media type, and its data
type icon struct {
	mediaType string
	data      []byte
}

var _ gofiler.Icon = icon{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// TypeSVG is the media type of an icon which is an SVG image
const TypeSVG = "image/svg+xml"

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an icon of the media type, such as TypeSVG, and its data, or
// nil, when there's no data
func New(mediaType string, data []byte) gofiler.Icon {
	if len(data) == 0 {
		return nil
	}
	return icon{mediaType: mediaType, data: data}
}

// SVG returns an icon of an SVG image, or nil, when there's no data
func SVG(data []byte) gofiler.Icon {
	return New(TypeSVG, data)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (i icon) Type() string { return i.mediaType }
func (i icon) Data() []byte { return i.data }
