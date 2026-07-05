package image

import (
	"io"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	exif "github.com/mutablelogic/go-media/pkg/exif"
	libexif "github.com/mutablelogic/go-media/sys/libexif"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ExtractExif reads EXIF metadata from r and returns the fields useful for
// indexing: orientation, camera make/model, lens, capture date and GPS
// location. Returns an empty slice if the image has no readable EXIF data.
func ExtractEXIF(r io.Reader) []schema.Meta {
	e, err := exif.Read(r)
	if err != nil {
		return nil
	}
	defer e.Close()

	// Index tags by name. Where a name appears more than once (e.g. IFD0
	// and the thumbnail IFD both carry Orientation), keep the first
	// occurrence, since IFD0 (the main image) is enumerated first.
	tags := e.Tags()
	tagmap := make(map[string]*exif.Tag, len(tags))
	for _, tag := range tags {
		if _, exists := tagmap[tag.Key()]; !exists {
			tagmap[tag.Key()] = tag
		}
	}

	var kv []schema.Meta
	if v, ok := exifString(tagmap, "Make"); ok {
		kv = schema.AppendMeta(kv, metadata.ImageMake, v)
	}
	if v, ok := exifString(tagmap, "Model"); ok {
		kv = schema.AppendMeta(kv, metadata.ImageModel, v)
	}
	if v, ok := exifString(tagmap, "LensModel"); ok {
		kv = schema.AppendMeta(kv, metadata.ImageLensModel, v)
	}
	if v, ok := exifOrientation(tagmap); ok {
		kv = schema.AppendMeta(kv, metadata.ImageOrientation, v)
	}
	if t, ok := exifDateTime(tagmap); ok {
		kv = schema.AppendMeta(kv, metadata.DateCreated, t.Format(time.RFC3339))
	}
	if lat, lng, ok := exifGPSLocation(tagmap); ok {
		kv = schema.AppendMeta(kv, metadata.ImageGPSLatitude, lat)
		kv = schema.AppendMeta(kv, metadata.ImageGPSLongitude, lng)
	}
	if alt, ok := exifGPSAltitude(tagmap); ok {
		kv = schema.AppendMeta(kv, metadata.ImageGPSAltitude, alt)
	}
	if v, ok := exifRational(tagmap, "FNumber"); ok {
		kv = schema.AppendMeta(kv, metadata.ImageAperture, v)
	}
	if v, ok := exifRational(tagmap, "ExposureTime"); ok {
		kv = schema.AppendMeta(kv, metadata.ImageShutterSpeed, v)
	}

	return kv
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func exifString(tags map[string]*exif.Tag, key string) (string, bool) {
	tag, ok := exifTag(tags, key)
	if !ok {
		return "", false
	}
	s, ok := tag.Any().(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

func exifTag(tags map[string]*exif.Tag, key string) (*exif.Tag, bool) {
	if tag, ok := tags[key]; ok {
		return tag, true
	}
	if tag, ok := tags["exif:"+key]; ok {
		return tag, true
	}
	if tag, ok := tags["tiff:"+key]; ok {
		return tag, true
	}
	return nil, false
}

// exifOrientation returns the raw EXIF orientation code (1-8).
func exifOrientation(tags map[string]*exif.Tag) (int, bool) {
	tag, ok := exifTag(tags, "Orientation")
	if !ok {
		return 0, false
	}
	if v, ok := tag.Any().(uint16); ok {
		return int(v), true
	}
	return 0, false
}

// exifDateTime returns the original capture time, preferring
// DateTimeOriginal (with its UTC offset, if present) over DateTime.
func exifDateTime(tags map[string]*exif.Tag) (time.Time, bool) {
	dt, ok := exifString(tags, "DateTimeOriginal")
	if !ok {
		dt, ok = exifString(tags, "DateTime")
	}
	if !ok {
		return time.Time{}, false
	}

	offset, hasOffset := exifString(tags, "OffsetTimeOriginal")
	if !hasOffset {
		offset, hasOffset = exifString(tags, "OffsetTime")
	}

	layout := "2006:01:02 15:04:05"
	if hasOffset {
		layout += "-07:00"
		dt += offset
	}

	t, err := time.Parse(layout, dt)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// exifGPSLocation returns the GPS coordinates in decimal degrees, with
// southern latitudes and western longitudes negative.
func exifGPSLocation(tags map[string]*exif.Tag) (lat, lng float64, ok bool) {
	latDMS, ok := exifRationalSlice(tags, "GPSLatitude")
	if !ok {
		return 0, 0, false
	}
	lngDMS, ok := exifRationalSlice(tags, "GPSLongitude")
	if !ok {
		return 0, 0, false
	}

	lat, ok = dmsToDecimal(latDMS)
	if !ok {
		return 0, 0, false
	}
	lng, ok = dmsToDecimal(lngDMS)
	if !ok {
		return 0, 0, false
	}

	if ref, ok := exifString(tags, "GPSLatitudeRef"); ok && strings.EqualFold(ref, "S") {
		lat = -lat
	}
	if ref, ok := exifString(tags, "GPSLongitudeRef"); ok && strings.EqualFold(ref, "W") {
		lng = -lng
	}

	return lat, lng, true
}

// exifGPSAltitude returns the altitude in metres, negative when below sea level.
func exifGPSAltitude(tags map[string]*exif.Tag) (float64, bool) {
	alt, ok := exifRational(tags, "GPSAltitude")
	if !ok {
		return 0, false
	}
	if refTag, ok := exifTag(tags, "GPSAltitudeRef"); ok {
		if b, ok := refTag.Any().(uint8); ok && b == 1 {
			alt = -alt
		}
	}
	return alt, true
}

func exifRational(tags map[string]*exif.Tag, key string) (float64, bool) {
	tag, ok := exifTag(tags, key)
	if !ok {
		return 0, false
	}
	r, ok := tag.Any().(libexif.Rational)
	if !ok {
		return 0, false
	}
	return rationalToFloat(r), true
}

func exifRationalSlice(tags map[string]*exif.Tag, key string) ([]libexif.Rational, bool) {
	tag, ok := exifTag(tags, key)
	if !ok {
		return nil, false
	}
	s, ok := tag.Any().([]libexif.Rational)
	return s, ok
}

// dmsToDecimal converts a [degrees, minutes, seconds] rational triple into
// decimal degrees.
func dmsToDecimal(dms []libexif.Rational) (float64, bool) {
	if len(dms) != 3 {
		return 0, false
	}
	deg := rationalToFloat(dms[0])
	min := rationalToFloat(dms[1])
	sec := rationalToFloat(dms[2])
	return deg + min/60 + sec/3600, true
}

func rationalToFloat(r libexif.Rational) float64 {
	if r.Denominator == 0 {
		return 0
	}
	return float64(r.Numerator) / float64(r.Denominator)
}
