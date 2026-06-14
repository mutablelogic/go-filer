package markdown

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	// Packages
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	text "github.com/mutablelogic/go-filer/extractor/text"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type srtextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(new(srtextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *srtextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`^(application/x-subrip|text/srt)$`)
}

func (e *srtextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
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

	var lines []string
	metadata, err := text.NewTextReader(f).Read(ctx, func(_ int, line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		return metadata, err
	}

	// Now summarize the text
	if metadata_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), fmt.Sprintf("This file contains subtitles in a file with path %q. Summarize the contents of the subtitles.", path)); err != nil {
		return metadata, err
	} else if len(metadata_) > 0 {
		metadata = append(metadata, metadata_...)
	}

	// Return the metadata
	return metadata, nil
}
