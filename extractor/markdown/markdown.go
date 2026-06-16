package markdown

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	// Packages
	extractor "github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	text "github.com/mutablelogic/go-filer/extractor/text"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type mdextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(new(mdextractor))
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
	metadata, err := text.NewTextReader(f).Read(ctx, func(_ int, line string) error {
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
		return metadata, err
	}

	// Now summarize the text
	if metadata_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), fmt.Sprintf("This is a markdown formatted file with filename %q.", filepath.Base(path))); err != nil {
		return metadata, err
	} else if len(metadata_) > 0 {
		metadata = append(metadata, metadata_...)
	}

	// Append the actual title
	if title != "" {
		metadata = schema.AppendMetadataKV(metadata, extractor.TextTitle, title)
	}

	// Return the metadata
	return metadata, nil
}
