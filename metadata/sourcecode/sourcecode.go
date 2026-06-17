package sourcecode

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

type codeextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	prompt = `
		This is a source code file. Summarize its purpose, and include the language type and class and public function names in the keywords.
		If the file represents a class, include the class name in the title. If it represents a function, include the function name in the title.
	`
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(codeextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *codeextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`text/(x-)?(go|python|javascript|typescript|java|c|cpp|objective-c|csharp|ruby|php|rust|swift)`)
}

func (e *codeextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	title := ""
	lines := []string{}
	kv, err := text.NewTextReader(r).Read(ctx, func(_ int, line string) error {
		lines = append(lines, line)
		if title == "" {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				title = strings.TrimSpace(strings.TrimLeft(trimmed, "/"))
			}
		}
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

	// Append the actual title
	if title != "" {
		kv = schema.AppendMeta(kv, metadata.TextTitle, title)
	}

	// Return the metadata
	return kv, nil
}
