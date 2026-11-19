package schema

import (
	"encoding/json"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Geometry(t *testing.T) {
	// A point is of its latitude and longitude, which is GeoJSON's
	// longitude, then latitude, and its altitude, when it's known
	point := NewPoint(51.5, -0.12)
	require.NoError(t, point.Validate())
	data, err := json.Marshal(point)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"Point","coordinates":[-0.12,51.5]}`, string(data))
	data, err = json.Marshal(point.WithAlt(30))
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"Point","coordinates":[-0.12,51.5,30]}`, string(data))
	assert.Equal(t, Bounds{MinLong: -0.12, MinLat: 51.5, MaxLong: -0.12, MaxLat: 51.5}, point.Bounds())

	// It's read again, as it was
	var read Geometry
	require.NoError(t, json.Unmarshal(data, &read))
	assert.Equal(t, point.WithAlt(30), read)

	// A polygon is of its rings, which are closed, and its box is of its
	// boundary, and a multipolygon is of more than one
	square := []Position{{Long: 0, Lat: 0}, {Long: 2, Lat: 0}, {Long: 2, Lat: 1}, {Long: 0, Lat: 1}, {Long: 0, Lat: 0}}
	hole := []Position{{Long: 0.5, Lat: 0.25}, {Long: 1, Lat: 0.25}, {Long: 1, Lat: 0.5}, {Long: 0.5, Lat: 0.25}}
	polygon := NewPolygon(square, hole)
	require.NoError(t, polygon.Validate())
	assert.Equal(t, Bounds{MinLong: 0, MinLat: 0, MaxLong: 2, MaxLat: 1}, polygon.Bounds())
	data, err = json.Marshal(polygon)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,1],[0,1],[0,0]],[[0.5,0.25],[1,0.25],[1,0.5],[0.5,0.25]]]}`, string(data))
	require.NoError(t, json.Unmarshal(data, &read))
	assert.Equal(t, polygon, read)

	far := []Position{{Long: 10, Lat: -5}, {Long: 11, Lat: -5}, {Long: 11, Lat: -4}, {Long: 10, Lat: -5}}
	multi := NewMultiPolygon([][]Position{square}, [][]Position{far})
	require.NoError(t, multi.Validate())
	assert.Equal(t, Bounds{MinLong: 0, MinLat: -5, MaxLong: 11, MaxLat: 1}, multi.Bounds())
	data, err = json.Marshal(multi)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &read))
	assert.Equal(t, multi, read)

	// Of a geometry which isn't valid, or another type of GeoJSON's
	for name, g := range map[string]Geometry{
		"latitude":   NewPoint(91, 0),
		"longitude":  NewPoint(0, -181),
		"open ring":  NewPolygon([]Position{{Long: 0, Lat: 0}, {Long: 1, Lat: 0}, {Long: 1, Lat: 1}, {Long: 0, Lat: 1}}),
		"short ring": NewPolygon([]Position{{Long: 0, Lat: 0}, {Long: 1, Lat: 0}, {Long: 0, Lat: 0}}),
		"no rings":   NewPolygon(),
		"none":       NewMultiPolygon(),
		"type":       {Type: "LineString"},
	} {
		assert.Error(t, g.Validate(), name)
	}
	assert.Error(t, json.Unmarshal([]byte(`{"type":"LineString","coordinates":[[0,0],[1,1]]}`), &read))
	assert.Error(t, json.Unmarshal([]byte(`{"type":"Point","coordinates":[1]}`), &read))
}
