package markdown

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
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
	Author   string   `json:"author,omitempty"`
	Title    string   `json:"title,omitempty"`
	Summary  string   `json:"summary,omitempty"`
	Keywords []string `json:"keywords,omitempty"`
	Language string   `json:"language,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	OllamaUrl            = "http://nestor.tailnet-db1f.ts.net:11434"
	OllamaModel          = "phi4"
	OllamaMaxInputTokens = 16384
	OllamaTokensPerWord  = 3.5
	SystemPrompt         = `
		Summarize the contents of the following text into a short paragraph in English, 
		with author, title, summary paragraph and ISO two-letter written language.
		Include key concepts, names, countries, regions and categories as keywords 
		when the text is substantive about those concepts. If any field is unknown,
		leave it blank.
	`
)

var (
	ollamaOnce           sync.Once
	ollamaClient         *ollama.Client
	ollamaModel          *llmschema.Model
	ollamaMaxInputTokens float64
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(textextractor))
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
			if model.InputTokenLimit != nil && types.Value(model.InputTokenLimit) > 0 {
				ollamaMaxInputTokens = float64(types.Value(model.InputTokenLimit))
			} else {
				ollamaMaxInputTokens = OllamaMaxInputTokens
			}
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

func (e *textextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.MetadataKV, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	// Open the file
	f, ok := r.(io.ReadCloser)
	if !ok {
		f = io.NopCloser(r)
	}
	defer f.Close()

	// Read the lines, capped by the model's token limit
	var lines []string
	metadata, err := NewTextReader(f).Read(ctx, func(num int, line string) error {
		lines = append(lines, line)
		return nil
	})
	if !errors.Is(err, io.EOF) && err != nil {
		return metadata, err
	}

	// Summarize the text
	if metadata_, err := summarizer.Summarize(ctx, strings.Join(lines, "\n")); err != nil {
		return metadata, err
	} else if len(metadata_) > 0 {
		metadata = append(metadata, metadata_...)
	}

	// Return the metadata
	return metadata, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - READER

func (r *textreader) Read(ctx context.Context, fn func(int, string) error) ([]schema.MetadataKV, error) {
	var tokens float64
	for r.scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		r.linecount++
		line := strings.TrimSpace(r.scanner.Text())
		if fn != nil {
			if err := fn(r.linecount, line); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
		}
		if line == "" {
			continue
		}
		tokens += float64(len(strings.Fields(line))) * OllamaTokensPerWord
		if ollamaMaxInputTokens > 0 && tokens > ollamaMaxInputTokens {
			break
		}
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}

	// Append the line count metadata
	metadata := schema.AppendMetadataKV([]schema.MetadataKV{}, metadata.TextLines, r.linecount)

	// Return the metadata
	return metadata, nil
}

func (r *textreader) LineCount() int {
	return r.linecount
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - SUMMARIZER

func (r *textsummarizer) Summarize(ctx context.Context, text string, prompts ...string) ([]schema.MetadataKV, error) {
	prompts = append([]string{SystemPrompt}, prompts...)
	opts := []llmopt.Opt{
		llmopt.AddString(llmopt.SystemPromptKey, strings.Join(prompts, "\n\n")),
		ollama.WithJSONOutput(jsonschema.MustFor[textsummarizer]()),
	}

	message, err := llmschema.NewMessage(llmschema.RoleUser, text)
	if err != nil {
		return nil, err
	}

	response, _, err := ollamaClient.WithoutSession(ctx, types.Value(ollamaModel), message, opts...)
	if err != nil {
		return nil, err
	} else if err := json.Unmarshal([]byte(response.Text()), r); err != nil {
		return nil, err
	}

	// Append the summarization metadata
	kv := []schema.MetadataKV{}
	kv = schema.AppendMetadataKV(kv, metadata.TextAuthor, r.Author)
	kv = schema.AppendMetadataKV(kv, metadata.TextTitle, r.Title)
	kv = schema.AppendMetadataKV(kv, metadata.TextSummary, r.Summary)
	kv = schema.AppendMetadataKV(kv, metadata.TextTags, r.Keywords)
	kv = schema.AppendMetadataKV(kv, metadata.TextLanguage, r.Language)

	// Return success
	return kv, nil
}
