package json

import (
	"context"
	"io"
	"regexp"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	text "github.com/mutablelogic/go-filer/metadata/text"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type jsonextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	prompt = "This is a JSON file. Summarize its purpose, and include any relevant keywords."
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(jsonextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *jsonextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`application/json`)
}

func (e *jsonextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	lines := []string{}
	kv, err := text.NewTextReader(r).Read(ctx, func(_ int, line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		return kv, err
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), prompt); err != nil {
		return kv, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	// Return the metadata
	return kv, nil
}
