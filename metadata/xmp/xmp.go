package xmp

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"regexp"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type xmpextractor struct{}

type xmpData struct {
	title    string
	summary  string
	author   []string
	keywords []string
	created  string
	modified string
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	dcNS  = "http://purl.org/dc/elements/1.1/"
	rdfNS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	xmpNS = "http://ns.adobe.com/xap/1.0/"
	psNS  = "http://ns.adobe.com/photoshop/1.0/"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(xmpextractor))
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - EXTRACTOR

func (e *xmpextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`application/xmp\+xml`)
}

func (e *xmpextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	dec := xml.NewDecoder(bytes.NewReader(data))
	var stack []xml.Name
	var liField string
	var liBuf strings.Builder
	var directField string
	var directName xml.Name
	var directBuf strings.Builder
	meta := new(xmpData)

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		switch token := tok.(type) {
		case xml.StartElement:
			for _, attr := range token.Attr {
				if field := directTextField(attr.Name); field != "" {
					setXMPField(meta, field, strings.TrimSpace(attr.Value))
				}
			}

			if directField == "" {
				if field := directTextField(token.Name); field != "" {
					directField = field
					directName = token.Name
					directBuf.Reset()
				}
			}
			if token.Name.Space == rdfNS && token.Name.Local == "li" {
				if field := containerField(stack); field != "" {
					liField = field
					liBuf.Reset()
				}
			}
			stack = append(stack, token.Name)

		case xml.CharData:
			if liField != "" {
				liBuf.Write(token)
			}
			if directField != "" {
				directBuf.Write(token)
			}

		case xml.EndElement:
			if len(stack) > 0 && stack[len(stack)-1] == token.Name {
				if liField != "" && token.Name.Space == rdfNS && token.Name.Local == "li" {
					setXMPField(meta, liField, strings.TrimSpace(liBuf.String()))
					liField = ""
					liBuf.Reset()
				}
				if directField != "" && token.Name == directName {
					setXMPField(meta, directField, strings.TrimSpace(directBuf.String()))
					directField = ""
					directName = xml.Name{}
					directBuf.Reset()
				}
				stack = stack[:len(stack)-1]
			}
		}
	}

	kv := []schema.Meta{}
	kv = schema.AppendMeta(kv, metadata.TextTitle, meta.title)
	kv = schema.AppendMeta(kv, metadata.TextAuthor, strings.Join(meta.author, ", "))
	kv = schema.AppendMeta(kv, metadata.TextSummary, meta.summary)
	kv = schema.AppendMeta(kv, metadata.TextTags, meta.keywords)
	kv = schema.AppendMeta(kv, metadata.DateCreated, meta.created)
	kv = schema.AppendMeta(kv, metadata.DateModified, meta.modified)
	return kv, nil, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func containerField(stack []xml.Name) string {
	for i := len(stack) - 1; i >= 0; i-- {
		switch stack[i].Space {
		case dcNS:
			switch stack[i].Local {
			case "title":
				return "title"
			case "description":
				return "summary"
			case "creator":
				return "author"
			case "subject":
				return "keywords"
			}
		}
	}
	return ""
}

func directTextField(name xml.Name) string {
	switch name.Space {
	case xmpNS:
		switch name.Local {
		case "CreateDate":
			return metadata.DateCreated
		case "ModifyDate":
			return metadata.DateModified
		}
	case psNS:
		switch name.Local {
		case "Headline":
			return "title"
		case "DateCreated":
			return metadata.DateCreated
		}
	}
	return ""
}

func setXMPField(meta *xmpData, field, value string) {
	if value == "" {
		return
	}

	switch field {
	case "title":
		if meta.title == "" {
			meta.title = value
		}
	case "summary":
		if meta.summary == "" {
			meta.summary = value
		}
	case "author":
		if !contains(meta.author, value) {
			meta.author = append(meta.author, value)
		}
	case "keywords":
		if isIgnoredKeyword(value) {
			return
		}
		if !contains(meta.keywords, value) {
			meta.keywords = append(meta.keywords, value)
		}
	case metadata.DateCreated:
		if meta.created == "" {
			meta.created = value
		}
	case metadata.DateModified:
		if meta.modified == "" {
			meta.modified = value
		}
	}
}

func contains(values []string, value string) bool {
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
}

func isIgnoredKeyword(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "all":
		return true
	default:
		return false
	}
}
