package markdown

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	extractor "github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
	llmopt "github.com/mutablelogic/go-llm/pkg/opt"
	ollama "github.com/mutablelogic/go-llm/provider/ollama"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type textextractor struct{}

type textreader struct {
	linecount int
	scanner   *bufio.Scanner
}

type textsummarizer struct {
	Title    string
	Summary  string
	Keywords []string
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	OllamaUrl   = "http://nestor.local:11434"
	OllamaModel = "phi4"
)

var (
	ollamaOnce   sync.Once
	ollamaClient *ollama.Client
	ollamaModel  *llmschema.Model
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(new(textextractor))
}

func NewTextReader(r io.Reader) *textreader {
	return &textreader{scanner: bufio.NewScanner(r)}
}

func NewTextSummarizer(ctx context.Context) (*textsummarizer, error) {
	var err error
	ollamaOnce.Do(func() {
		if client, err_ := ollama.New(OllamaUrl, client.OptTimeout(5*time.Minute)); err_ != nil {
			err = err_
			return
		} else if model, err_ := client.GetModel(ctx, OllamaModel); err_ != nil {
			err = err_
			return
		} else {
			ollamaClient = client
			ollamaModel = model
		}
	})
	if err != nil {
		return nil, err
	}
	return new(textsummarizer), nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - EXTRACTOR

func (e *textextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`text/plain`)
}

func (e *textextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	// Open the file
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the lines, summarize the text and extract metadata
	return NewTextReader(f).Read(ctx, nil)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - READER

func (r *textreader) Read(ctx context.Context, fn func(int, string) error) ([]schema.MetadataKV, error) {
	var text string
	for r.scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		text += r.scanner.Text() + "\n"
		r.linecount++
		if fn == nil {
			continue
		}
		if err := fn(r.linecount, strings.TrimSpace(r.scanner.Text())); err != nil {
			return nil, err
		}
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	// Append the line count metadata
	metadata := schema.AppendMetadataKV([]schema.MetadataKV{}, extractor.TextLines, r.linecount)

	// Now summarize the text
	summarizer, err := NewTextSummarizer(ctx)
	if err != nil {
		return metadata, err
	} else if err := summarizer.Summarize(ctx, text); err != nil {
		return metadata, err
	}

	// Append the summarization metadata
	metadata = schema.AppendMetadataKV(metadata, extractor.TextTitle, summarizer.Title)
	metadata = schema.AppendMetadataKV(metadata, extractor.TextSummary, summarizer.Summary)
	if len(summarizer.Keywords) > 0 {
		metadata = schema.AppendMetadataKV(metadata, extractor.TextTags, summarizer.Keywords)
	}

	// Return the metadata
	return metadata, nil
}

func (r *textreader) LineCount() int {
	return r.linecount
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - SUMMARIZER

func (r *textsummarizer) Summarize(ctx context.Context, text string) error {
	opts := []llmopt.Opt{
		llmopt.AddString(llmopt.SystemPromptKey, "Summarize the purpose of the following text into a short paragraph in English, with title, summary paragraph. Include concepts, names, countries, regions and categories as keywords when the text is substantive about those concepts."),
		ollama.WithJSONOutput(jsonschema.MustFor[textsummarizer]()),
	}

	message, err := llmschema.NewMessage(llmschema.RoleUser, text)
	if err != nil {
		return err
	}

	response, _, err := ollamaClient.WithoutSession(ctx, types.Value(ollamaModel), message, opts...)
	if err != nil {
		return err
	} else if err := json.Unmarshal([]byte(response.Text()), r); err != nil {
		return err
	}

	// Return success
	return nil
}
