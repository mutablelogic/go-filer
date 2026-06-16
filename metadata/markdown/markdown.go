package markdown

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	return regexp.MustCompile(`text/markdown`)
}

func (e *mdextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	title := ""
	lines := []string{}
	kv, err := text.NewTextReader(f).Read(ctx, func(_ int, line string) error {
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
		return kv, err
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), fmt.Sprintf("This is a markdown formatted file with filename %q.", filepath.Base(path))); err != nil {
		return kv, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	// Append the actual title
	if title != "" {
		kv = schema.AppendMetadataKV(kv, metadata.TextTitle, title)
	}

	// Return the metadata
	return kv, nil
}
