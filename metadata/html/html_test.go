package html

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metadata "github.com/mutablelogic/go-filer/metadata"
)

func TestExtractMetadata(t *testing.T) {
	extractor, err := metadata.Get("text/html")
	if err != nil {
		t.Fatal(err)
	}

	const input = `<!doctype html>
<html>
  <head>
    <title>Example Page</title>
    <meta charset="utf-8">
    <meta name="description" content="A short summary">
    <meta property="og:title" content="Open Graph Title">
    <meta http-equiv="refresh" content="30">
    <meta itemprop="author" content="Ada Lovelace">
  </head>
  <body></body>
</html>`

	kv, err := extractor.ExtractMetadata(context.Background(), strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}

	got := make(map[string][]string)
	for _, meta := range kv {
		got[meta.Key] = append(got[meta.Key], string(meta.Value))
	}

	decode := func(key string) string {
		var value string
		if err := json.Unmarshal([]byte(got[key][0]), &value); err != nil {
			t.Fatalf("decode %s: %v", key, err)
		}
		return value
	}

	if decode(metadata.TextTitle) != "Example Page" {
		t.Fatalf("title = %q, want %q", got[metadata.TextTitle][0], "Example Page")
	}
	if decode("charset") != "utf-8" {
		t.Fatalf("charset = %q, want %q", got["charset"][0], "utf-8")
	}
	if decode("description") != "A short summary" {
		t.Fatalf("description = %q, want %q", got["description"][0], "A short summary")
	}
	if decode("og:title") != "Open Graph Title" {
		t.Fatalf("og:title = %q, want %q", got["og:title"][0], "Open Graph Title")
	}
	if decode("refresh") != "30" {
		t.Fatalf("refresh = %q, want %q", got["refresh"][0], "30")
	}
	if decode("author") != "Ada Lovelace" {
		t.Fatalf("author = %q, want %q", got["author"][0], "Ada Lovelace")
	}
}
