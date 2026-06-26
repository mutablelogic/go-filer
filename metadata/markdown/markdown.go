package markdown

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

type mdextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(mdextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *mdextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`^text/markdown$`)
}

func (e *mdextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, nil, err
	}

	title := ""
	lines := []string{}
	kv, err := text.NewTextReader(r).Read(ctx, func(_ int, line string) error {
		lines = append(lines, line)
		if title == "" {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "#") {
				title = strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			}
		}
		return nil
	})
	if err != nil {
		return kv, nil, err
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), "This is markdown content."); err != nil {
		return kv, nil, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	// Append the actual title
	if title != "" {
		kv = schema.AppendMeta(kv, metadata.TextTitle, title)
	}

	// Return the metadata
	return kv, nil, nil
}
