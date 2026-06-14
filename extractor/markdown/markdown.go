package markdown

import (
	"context"
	"os"
	"regexp"
	"strings"

	// Packages
	extractor "github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	text "github.com/mutablelogic/go-filer/extractor/text"
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
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lineCount := 0
	title := ""

	metadata, err := text.NewTextReader(f).Read(ctx, func(num int, line string) error {
		_ = num
		lineCount++

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

	// Append the actual title
	if title != "" {
		metadata = schema.AppendMetadataKV(metadata, extractor.TextTitle, title)
	}

	// Return the metadata
	return metadata, nil
}
