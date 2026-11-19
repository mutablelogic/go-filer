//go:build !cgo

package enrichment

import (
	// The handler of Photoshop documents, which is pure Go without cgo, but
	// reads their EXIF with libexif with it, so it's of the image handlers
	// then (see handlers_cgo.go)
	_ "github.com/mutablelogic/go-metadata/image/photoshop"
)
