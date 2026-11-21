package metadata_test

import (
	"context"
	"strings"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	metadata "github.com/mutablelogic/go-filer/metadata"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Markdown(t *testing.T) {
	m, err := metadata.New()
	require.NoError(t, err)
	ctx := context.Background()

	// A document is read as Markdown, of its start
	page := `<html><head><title>Page</title></head><body><h1>Hello</h1><p>A paragraph of <b>text</b>.</p></body></html>`
	md, err := m.Markdown(ctx, strings.NewReader(page), &schema.Object{ObjectKey: schema.ObjectKey{Name: "media", Path: "/page.html"}, ObjectMeta: schema.ObjectMeta{ContentType: "text/html"}})
	require.NoError(t, err)
	assert.Contains(t, md.Body, "Hello")
	assert.Contains(t, md.Body, "paragraph")
	assert.Zero(t, md.Next)

	// Bytes which aren't a document can't be
	_, err = m.Markdown(ctx, strings.NewReader("\x00\x01\x02\x03"), &schema.Object{ObjectKey: schema.ObjectKey{Name: "media", Path: "/a.bin"}})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
}
