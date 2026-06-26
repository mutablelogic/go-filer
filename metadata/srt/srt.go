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

type srtextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(srtextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *srtextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`^(application/x-subrip|text/srt)$`)
}

func (e *srtextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, nil, err
	}

	var lines []string
	kv, err := text.NewTextReader(r).Read(ctx, func(_ int, line string) error {
		lines = append(lines, line)
		return nil
	})
	if err != nil {
		return kv, nil, err
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n"), "This content contains subtitles in SRT format. Summarize the subtitle content in English."); err != nil {
		return kv, nil, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	// Return the metadata
	return kv, nil, nil
}
