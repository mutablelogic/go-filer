package image

import (
	"bytes"
	"image"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"slices"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	_ "golang.org/x/image/bmp"
	xdraw "golang.org/x/image/draw"
	_ "golang.org/x/image/tiff"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// The maximum width of the artwork to extract. If the image is larger than this, it will be resized,
	// but the aspect ratio will be preserved. If the image is smaller than this, it will be returned as is,
	// but potentially converted to a jpeg.
	MaxWidth = 640

	// Formats that should be encoded as PNG when resized. All other formats are encoded as JPEG.
	PNGFormats = "png,gif,bmp,tiff"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateArtwork converts an image into a schema.ArtworkMeta object
// and return the metadata for the original image
func CreateArtwork(r io.Reader) (*schema.ArtworkMeta, []schema.Meta, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	// Decode the image
	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}

	// Generate metdata for the original image
	kv := schema.AppendMeta([]schema.Meta{}, metadata.ImageFormat, format)
	kv = schema.AppendMeta(kv, metadata.ImageWidth, img.Bounds().Dx())
	kv = schema.AppendMeta(kv, metadata.ImageHeight, img.Bounds().Dy())

	// Fast-path: if the image is already a jpeg or png and approximately the max width, return it as is
	if (format == "png" || format == "jpeg") && img.Bounds().Dx() <= int(float64(MaxWidth)*1.1) {
		return &schema.ArtworkMeta{
			Data:   data,
			Type:   "image/" + format,
			Width:  uint64(img.Bounds().Dx()),
			Height: uint64(img.Bounds().Dy()),
		}, kv, nil
	}

	// Preserve the aspect ratio and resize the image to the max width
	width := min(MaxWidth, img.Bounds().Dx())
	height := img.Bounds().Dy() * width / img.Bounds().Dx()
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	xdraw.BiLinear.Scale(dst, dst.Rect, img, img.Bounds(), xdraw.Over, nil)

	// If the original was a lossless format, encode as png; otherwise encode as jpeg
	var buf bytes.Buffer
	var contentType string
	if slices.Contains(strings.Split(PNGFormats, ","), format) {
		if err = png.Encode(&buf, dst); err != nil {
			return nil, kv, err
		}
		contentType = "image/png"
	} else {
		if err = jpeg.Encode(&buf, dst, nil); err != nil {
			return nil, kv, err
		}
		contentType = "image/jpeg"
	}

	// Return the artwork meta
	return &schema.ArtworkMeta{
		Data:   buf.Bytes(),
		Type:   contentType,
		Width:  uint64(width),
		Height: uint64(height),
	}, kv, nil
}
