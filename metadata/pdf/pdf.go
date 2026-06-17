package pdf

import (
	"context"
	"io"
	"regexp"
	"strings"

	// Packages
	reader "github.com/carlos7ags/folio/reader"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	text "github.com/mutablelogic/go-filer/metadata/text"
	llm "github.com/mutablelogic/go-llm"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type pdfextractor struct {
	client llm.Generator
	model  *llmschema.Model
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	maxTextSize = 64 * 1024
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(pdfextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *pdfextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`application/pdf`)
}

// Extract metadata from the file at the given path
func (e *pdfextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	pdf, err := reader.Parse(data)
	if err != nil {
		return nil, err
	}
	title, author, subject, creator, producer := pdf.Info()

	kv := schema.AppendMeta([]schema.Meta{}, metadata.PDFTitle, title)
	kv = schema.AppendMeta(kv, metadata.PDFAuthor, author)
	kv = schema.AppendMeta(kv, metadata.PDFSubject, subject)
	kv = schema.AppendMeta(kv, metadata.PDFCreator, creator)
	kv = schema.AppendMeta(kv, metadata.PDFProducer, producer)
	kv = schema.AppendMeta(kv, metadata.PDFPages, pdf.PageCount())

	// Summarize the PDF by collecting page text up to 64K.
	var builder strings.Builder
	for i := 0; i < pdf.PageCount() && builder.Len() < maxTextSize; i++ {
		if err := ctx.Err(); err != nil {
			return kv, err
		}

		page, err := pdf.Page(i)
		if err != nil {
			continue
		}

		pageText, err := page.ExtractText()
		if err != nil || strings.TrimSpace(pageText) == "" {
			continue
		}

		remaining := maxTextSize - builder.Len()
		if len(pageText) > remaining {
			pageText = pageText[:remaining]
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(pageText)
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, builder.String()); err != nil {
		return kv, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	return kv, nil
}

/*
func sanitizeUnicode(s string) string {
	// Ensure valid UTF-8 and remove NUL bytes, which PostgreSQL JSONB rejects.
	b := bytes.ToValidUTF8([]byte(s), []byte(""))
	b = bytes.ReplaceAll(b, []byte{0}, nil)
	return string(b)
}
*/
