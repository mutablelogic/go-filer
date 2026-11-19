package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Geometry is a shape of where an object is, such as where a photo was
// taken, of GeoJSON (RFC 7946): a point, or a polygon, or more than one
// polygon, whose positions are of longitude, then latitude, in degrees
// (WGS 84), as GeoJSON's are
type Geometry struct {
	Type GeometryType

	// The position of a point
	Point Position

	// The polygons, of a polygon, which is one, or of a multipolygon: of
	// each, its rings, the first its boundary, and the others its holes,
	// each of which is closed, as its last position is its first
	Polygons [][][]Position
}

// GeometryType is the type of a geometry, of GeoJSON's
type GeometryType string

// Position is a position of a geometry, of its longitude, and latitude, in
// degrees, and its altitude, in metres, when it's known, which is of
// JSON as an array of them, in that order, as GeoJSON's is
type Position struct {
	Long float64
	Lat  float64
	Alt  *float64
}

// Bounds is the box of a geometry, of the least and greatest of its
// longitudes and latitudes, which a point's is the point
type Bounds struct {
	MinLong float64 `json:"min_long"`
	MinLat  float64 `json:"min_lat"`
	MaxLong float64 `json:"max_long"`
	MaxLat  float64 `json:"max_lat"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	GeometryPoint        GeometryType = "Point"
	GeometryPolygon      GeometryType = "Polygon"
	GeometryMultiPolygon GeometryType = "MultiPolygon"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewPoint returns the geometry of a point, of its latitude and longitude,
// in that order, as positions usually are, such as of a photo's GPS
func NewPoint(lat, long float64) Geometry {
	return Geometry{Type: GeometryPoint, Point: Position{Long: long, Lat: lat}}
}

// NewPolygon returns the geometry of a polygon, of its rings, its boundary
// first, then its holes
func NewPolygon(rings ...[]Position) Geometry {
	return Geometry{Type: GeometryPolygon, Polygons: [][][]Position{rings}}
}

// NewMultiPolygon returns the geometry of more than one polygon, of each
// one's rings
func NewMultiPolygon(polygons ...[][]Position) Geometry {
	return Geometry{Type: GeometryMultiPolygon, Polygons: polygons}
}

// WithAlt returns the point with its altitude, in metres
func (g Geometry) WithAlt(alt float64) Geometry {
	g.Point.Alt = &alt
	return g
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Validate returns an error when the geometry isn't one of its type:
// a point, or polygons whose rings are closed, of four positions or more,
// of positions which are of the earth, whose longitudes are -180 to 180,
// and latitudes -90 to 90
func (g Geometry) Validate() error {
	switch g.Type {
	case GeometryPoint:
		if len(g.Polygons) > 0 {
			return geoErr("a point has no polygons")
		}
		return g.Point.Validate()
	case GeometryPolygon, GeometryMultiPolygon:
		if len(g.Polygons) == 0 {
			return geoErr("a %s has no polygons", g.Type)
		} else if g.Type == GeometryPolygon && len(g.Polygons) != 1 {
			return geoErr("a polygon is one polygon")
		}
		for _, polygon := range g.Polygons {
			if len(polygon) == 0 {
				return geoErr("a polygon has no rings")
			}
			for _, ring := range polygon {
				if len(ring) < 4 {
					return geoErr("a polygon's ring has %d positions, rather than four or more", len(ring))
				} else if first, last := ring[0], ring[len(ring)-1]; first.Long != last.Long || first.Lat != last.Lat {
					return geoErr("a polygon's ring isn't closed, as its last position isn't its first")
				}
				for _, position := range ring {
					if err := position.Validate(); err != nil {
						return err
					}
				}
			}
		}
		return nil
	default:
		return geoErr("a geometry of type %q, rather than a Point, Polygon or MultiPolygon", g.Type)
	}
}

// Bounds returns the box of the geometry, of its points, and of its
// polygons' boundaries, as their holes are within them
func (g Geometry) Bounds() Bounds {
	if g.Type == GeometryPoint {
		return Bounds{MinLong: g.Point.Long, MinLat: g.Point.Lat, MaxLong: g.Point.Long, MaxLat: g.Point.Lat}
	}
	b := Bounds{MinLong: math.Inf(1), MinLat: math.Inf(1), MaxLong: math.Inf(-1), MaxLat: math.Inf(-1)}
	for _, polygon := range g.Polygons {
		if len(polygon) == 0 {
			continue
		}
		for _, position := range polygon[0] {
			b.MinLong, b.MaxLong = min(b.MinLong, position.Long), max(b.MaxLong, position.Long)
			b.MinLat, b.MaxLat = min(b.MinLat, position.Lat), max(b.MaxLat, position.Lat)
		}
	}
	return b
}

// Validate returns an error when the position isn't of the earth
func (p Position) Validate() error {
	if math.IsNaN(p.Long) || p.Long < -180 || p.Long > 180 {
		return geoErr("a longitude of %v, rather than -180 to 180", p.Long)
	} else if math.IsNaN(p.Lat) || p.Lat < -90 || p.Lat > 90 {
		return geoErr("a latitude of %v, rather than -90 to 90", p.Lat)
	} else if p.Alt != nil && (math.IsNaN(*p.Alt) || math.IsInf(*p.Alt, 0)) {
		return geoErr("an altitude of %v", *p.Alt)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// JSON

// geometryJSON is a geometry of GeoJSON
type geometryJSON struct {
	Type        GeometryType    `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// MarshalJSON returns the geometry as GeoJSON
func (g Geometry) MarshalJSON() ([]byte, error) {
	var coordinates any
	switch g.Type {
	case GeometryPoint:
		coordinates = g.Point
	case GeometryPolygon:
		if len(g.Polygons) != 1 {
			return nil, geoErr("a polygon is one polygon")
		}
		coordinates = g.Polygons[0]
	case GeometryMultiPolygon:
		coordinates = g.Polygons
	default:
		return nil, geoErr("a geometry of type %q", g.Type)
	}
	data, err := json.Marshal(coordinates)
	if err != nil {
		return nil, err
	}
	return json.Marshal(geometryJSON{Type: g.Type, Coordinates: data})
}

// UnmarshalJSON reads the geometry of GeoJSON, of a Point, Polygon or
// MultiPolygon
func (g *Geometry) UnmarshalJSON(data []byte) error {
	var v geometryJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	result := Geometry{Type: v.Type}
	var err error
	switch v.Type {
	case GeometryPoint:
		err = json.Unmarshal(v.Coordinates, &result.Point)
	case GeometryPolygon:
		var rings [][]Position
		if err = json.Unmarshal(v.Coordinates, &rings); err == nil {
			result.Polygons = [][][]Position{rings}
		}
	case GeometryMultiPolygon:
		err = json.Unmarshal(v.Coordinates, &result.Polygons)
	default:
		return geoErr("a geometry of type %q, rather than a Point, Polygon or MultiPolygon", v.Type)
	}
	if err != nil {
		return err
	}
	*g = result
	return nil
}

// MarshalJSON returns the position as an array of its longitude, latitude,
// and its altitude, when it's known
func (p Position) MarshalJSON() ([]byte, error) {
	if p.Alt != nil {
		return json.Marshal([]float64{p.Long, p.Lat, *p.Alt})
	}
	return json.Marshal([]float64{p.Long, p.Lat})
}

// UnmarshalJSON reads the position of an array of its longitude, latitude,
// and altitude, when it has one
func (p *Position) UnmarshalJSON(data []byte) error {
	var v []float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	} else if len(v) < 2 || len(v) > 3 {
		return geoErr("a position of %d numbers, rather than its longitude, latitude, and altitude", len(v))
	}
	*p = Position{Long: v[0], Lat: v[1]}
	if len(v) == 3 {
		p.Alt = &v[2]
	}
	return nil
}

// String returns the geometry as GeoJSON
func (g Geometry) String() string {
	data, err := json.Marshal(g)
	if err != nil {
		return fmt.Sprint(err)
	}
	return string(bytes.TrimSpace(data))
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// geoErr returns an error of a geometry which isn't valid
func geoErr(format string, args ...any) error {
	return fmt.Errorf("invalid geometry: "+format, args...)
}
