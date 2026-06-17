package html

import (
	"bytes"
	"context"
	stdhtml "html"
	"io"
	"regexp"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	text "github.com/mutablelogic/go-filer/metadata/text"
	xhtml "golang.org/x/net/html"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type htmlextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(htmlextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *htmlextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`text/(x)?html`)
}

func (e *htmlextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	doc, err := xhtml.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	kv := []schema.Meta{}
	title := ""
	walk(doc, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}

		switch strings.ToLower(node.Data) {
		case "title":
			if title == "" {
				title = strings.TrimSpace(textContent(node))
			}
		case "script", "style", "noscript":
			return
		case "meta":
			if key, value := metaFields(node); key != "" {
				kv = schema.AppendMeta(kv, key, value)
			}
		}
	})

	if title != "" {
		kv = schema.AppendMeta(kv, metadata.TextTitle, title)
	}

	if body := findElement(doc, "body"); body != nil {
		if text := strings.TrimSpace(visibleText(body)); text != "" {
			if summary, err := summarizer.Summarize(ctx, text, "This is an HTML page. Summarize the visible text content in English, with title, summary paragraph, and keywords when relevant. If any field is unknown, leave it blank."); err != nil {
				return kv, err
			} else if len(summary) > 0 {
				kv = append(kv, summary...)
			}
		}
	}

	return kv, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func walk(node *xhtml.Node, fn func(*xhtml.Node)) {
	if node == nil {
		return
	}
	fn(node)
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		walk(child, fn)
	}
}

func textContent(node *xhtml.Node) string {
	var builder strings.Builder
	walk(node, func(n *xhtml.Node) {
		if n.Type == xhtml.TextNode {
			builder.WriteString(n.Data)
		}
	})
	return stdhtml.UnescapeString(builder.String())
}

func visibleText(node *xhtml.Node) string {
	var builder strings.Builder
	collectVisibleText(node, &builder)
	return builder.String()
}

func collectVisibleText(node *xhtml.Node, builder *strings.Builder) {
	if node == nil {
		return
	}
	if node.Type == xhtml.ElementNode {
		switch strings.ToLower(node.Data) {
		case "script", "style", "noscript":
			return
		}
	}
	if node.Type == xhtml.TextNode {
		text := strings.TrimSpace(stdhtml.UnescapeString(node.Data))
		if text != "" {
			if builder.Len() > 0 {
				builder.WriteByte(' ')
			}
			builder.WriteString(text)
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectVisibleText(child, builder)
	}
}

func findElement(node *xhtml.Node, name string) *xhtml.Node {
	if node == nil {
		return nil
	}
	if node.Type == xhtml.ElementNode && strings.EqualFold(node.Data, name) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findElement(child, name); found != nil {
			return found
		}
	}
	return nil
}

func metaFields(node *xhtml.Node) (string, string) {
	attrs := make(map[string]string, len(node.Attr))
	for _, attr := range node.Attr {
		key := strings.ToLower(strings.TrimSpace(attr.Key))
		value := strings.TrimSpace(attr.Val)
		if key != "" {
			attrs[key] = value
		}
	}

	if value := strings.TrimSpace(attrs["charset"]); value != "" {
		return "charset", value
	}

	key := strings.TrimSpace(attrs["name"])
	if key == "" {
		key = strings.TrimSpace(attrs["property"])
	}
	if key == "" {
		key = strings.TrimSpace(attrs["itemprop"])
	}
	if key == "" {
		key = strings.TrimSpace(attrs["http-equiv"])
	}
	if key == "" {
		return "", ""
	}

	value := strings.TrimSpace(attrs["content"])
	if value == "" {
		value = strings.TrimSpace(attrs["charset"])
	}
	if value == "" {
		return "", ""
	}

	return strings.ToLower(key), value
}
