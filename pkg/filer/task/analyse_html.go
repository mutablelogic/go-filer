package task

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	htmlparser "github.com/mutablelogic/go-filer/pkg/htmlparser"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) AnalyseHTML(ctx context.Context, object *schema.Object) error {
	var buf bytes.Buffer

	// Read the text object into a buffer
	if _, err := t.filer.WriteObject(ctx, &buf, object.Bucket, object.Key); err != nil {
		return err
	}

	// Set up the object
	meta := schema.MediaMeta{
		Title: types.StringPtr(strings.TrimSuffix(filepath.Base(object.Key), filepath.Ext(object.Key))),
		Type:  "html",
		Meta:  make(map[string]any, 10),
	}

	// This is the document
	var doc strings.Builder

	// Set the rules for the parser
	parser := htmlparser.New()
	parser.AddRule("//head/title/text()", func(node htmlparser.Node) error {
		if title := strings.TrimSpace(node.Data); title != "" {
			meta.Title = types.StringPtr(title)
		}
		return nil
	})
	parser.AddRule("//head/meta", func(node htmlparser.Node) error {
		var name, content string
		for _, attr := range node.Attr {
			key := strings.ToLower(attr.Key)
			switch key {
			case "name":
				name = strings.ToLower(attr.Val)
			case "content":
				content = strings.TrimSpace(attr.Val)
			}
		}
		if name != "" && content != "" {
			meta.Meta[name] = content
		}
		return nil
	})
	parser.AddRule("//body//br", func(node htmlparser.Node) error {
		if doc.Len() > 0 {
			doc.WriteString("\n")
		}
		return nil
	})
	parser.AddRule("//body//p", func(node htmlparser.Node) error {
		if doc.Len() > 0 {
			doc.WriteString("\n\n")
		}
		return nil
	})
	parser.AddRule("//body//div", func(node htmlparser.Node) error {
		if doc.Len() > 0 {
			doc.WriteString("\n\n")
		}
		return nil
	})
	parser.AddRule("//body//text()", func(node htmlparser.Node) error {
		if text := strings.TrimSpace(node.Data); text != "" {
			doc.WriteString(text + " ")
		}
		return nil
	})

	// Parse the HTML
	if err := parser.Read(&buf); err != nil {
		return err
	}

	// Set description
	if desc, ok := meta.Meta["description"].(string); ok && desc != "" {
		meta.Description = types.StringPtr(desc)
	}

	// Insert the media object into the database
	media, err := t.filer.CreateMedia(ctx, object.Bucket, object.Key, meta)
	if err != nil {
		return err
	}

	// Insert a fragment for the text
	if doc.Len() > 0 {
		text := schema.MediaFragmentMeta{
			Type: "text",
			Text: doc.String(),
		}
		if _, err := t.filer.CreateMediaFragments(ctx, object.Bucket, object.Key, []schema.MediaFragmentMeta{text}); err != nil {
			return err
		}
	}

	// Output media update info
	ref.Log(ctx).With("media", media).Debugf(ctx, "Updated media with HTML properties")

	// Return sucess
	return nil
}
