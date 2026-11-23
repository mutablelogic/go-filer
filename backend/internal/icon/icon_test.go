package icon_test

import (
	"testing"

	// Packages
	icon "github.com/mutablelogic/go-filer/backend/internal/icon"
	assert "github.com/stretchr/testify/assert"
)

func Test_Icon(t *testing.T) {
	// An icon is of its media type, and its data
	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg"/>`)
	i := icon.SVG(svg)
	if assert.NotNil(t, i) {
		assert.Equal(t, icon.TypeSVG, i.Type())
		assert.Equal(t, svg, i.Data())
	}
	assert.Equal(t, "image/png", icon.New("image/png", []byte{1}).Type())

	// Without data, there's none
	assert.Nil(t, icon.SVG(nil))
	assert.Nil(t, icon.New("image/png", []byte{}))
}
