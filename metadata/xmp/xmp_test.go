package xmp

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestExtractMetadata(t *testing.T) {
	const sample = `<?xpacket begin="\ufeff" id="W5M0MpCehiHzreSzNTczkc9d"?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
           xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:xmp="http://ns.adobe.com/xap/1.0/">
    <rdf:Description rdf:about=""
      dc:title=""
      xmp:CreateDate="2026-06-19T08:00:00Z"
      xmp:ModifyDate="2026-06-19T08:01:00Z"
      xmp:MetadataDate="2026-06-19T08:02:00Z">
      <dc:title>
        <rdf:Alt>
          <rdf:li xml:lang="x-default">Sample Title</rdf:li>
        </rdf:Alt>
      </dc:title>
      <dc:creator>
        <rdf:Seq>
          <rdf:li>Jane Doe</rdf:li>
          <rdf:li>John Doe</rdf:li>
        </rdf:Seq>
      </dc:creator>
      <dc:description>
        <rdf:Alt>
          <rdf:li xml:lang="x-default">Sample summary.</rdf:li>
        </rdf:Alt>
      </dc:description>
      <dc:subject>
        <rdf:Bag>
					<rdf:li>All</rdf:li>
          <rdf:li>alpha</rdf:li>
          <rdf:li>beta</rdf:li>
        </rdf:Bag>
      </dc:subject>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>`

	meta, err := new(xmpextractor).ExtractMetadata(context.Background(), bytes.NewBufferString(sample))
	if err != nil {
		t.Fatalf("ExtractMetadata() error = %v", err)
	}

	got := map[string]any{}
	for _, kv := range meta {
		var value any
		if err := json.Unmarshal(kv.Value, &value); err != nil {
			t.Fatalf("unmarshal %q: %v", kv.Key, err)
		}
		got[kv.Key] = value
	}

	if got["title"] != "Sample Title" {
		t.Fatalf("title = %#v", got["title"])
	}
	if got["summary"] != "Sample summary." {
		t.Fatalf("summary = %#v", got["summary"])
	}
	if got["author"] != "Jane Doe, John Doe" {
		t.Fatalf("author = %#v", got["author"])
	}
	if tags, ok := got["tags"].([]any); !ok || len(tags) != 2 {
		t.Fatalf("tags = %#v", got["tags"])
	} else if tags[0] == "All" || tags[1] == "All" {
		t.Fatalf("tags should exclude generic 'All': %#v", tags)
	}
	if got["created"] != "2026-06-19T08:00:00Z" {
		t.Fatalf("created = %#v", got["created"])
	}
}

func TestExtractMetadataPhotoshopDateCreated(t *testing.T) {
	const sample = `<x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="XMP Core 6.0.0">
   <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <rdf:Description rdf:about=""
            xmlns:dc="http://purl.org/dc/elements/1.1/"
            xmlns:photoshop="http://ns.adobe.com/photoshop/1.0/">
         <dc:subject>
            <rdf:Seq>
               <rdf:li>All</rdf:li>
               <rdf:li>Karen</rdf:li>
            </rdf:Seq>
         </dc:subject>
         <photoshop:DateCreated>2001-01-21T20:43:05+01:00</photoshop:DateCreated>
      </rdf:Description>
   </rdf:RDF>
</x:xmpmeta>`

	meta, err := new(xmpextractor).ExtractMetadata(context.Background(), bytes.NewBufferString(sample))
	if err != nil {
		t.Fatalf("ExtractMetadata() error = %v", err)
	}

	got := map[string]any{}
	for _, kv := range meta {
		var value any
		if err := json.Unmarshal(kv.Value, &value); err != nil {
			t.Fatalf("unmarshal %q: %v", kv.Key, err)
		}
		got[kv.Key] = value
	}

	if got["created"] != "2001-01-21T20:43:05+01:00" {
		t.Fatalf("created = %#v", got["created"])
	}
	if tags, ok := got["tags"].([]any); !ok || len(tags) != 1 || tags[0] != "Karen" {
		t.Fatalf("tags = %#v", got["tags"])
	}
}
