package schema

import (
	"strings"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

func Test_searchFunctions(t *testing.T) {
	// The name of an object, of a file, a folder, or the root
	assert.Equal(t, "b.txt", searchName("/a/b.txt"))
	assert.Equal(t, "b", searchName("/a/b/"))
	assert.Equal(t, "", searchName("/"))

	// Its folders, nearest first, apart, and those above them, but not
	// those of fewer than three characters
	const key = "/Photos/ab/2026/Bern/a.jpg"
	assert.Equal(t, "Bern", searchFolder(key, 1))
	assert.Equal(t, "2026", searchFolder(key, 2))
	assert.Equal(t, "Photos", searchPath(key), "not ab")
	assert.Equal(t, "", searchFolder(key, 5))
	assert.Equal(t, "Bern", searchFolder("/Photos/Bern/a/", 1), "of a folder, its folder")
	assert.Equal(t, "", searchFolder("/a.jpg", 1))

	// The text of metadata, of strings, arrays, and objects, but not
	// numbers, booleans, or links, up to its most
	assert.Equal(t, "A", searchText(`"A"`))
	assert.Equal(t, "A B", searchText(`["A", 1, true, "https://example.com", "B"]`))
	assert.ElementsMatch(t, []string{"Meeting", "Berlin"}, strings.Fields(searchText(`[{"title": "Meeting", "location": "Berlin", "url": "https://x"}]`)))
	assert.Equal(t, "", searchText(`42`))
	assert.Equal(t, "", searchText(`not json`))
	assert.Len(t, searchText(`"`+strings.Repeat("ä", maxSearchText)+`"`), maxSearchText, "cut at a character")
}
