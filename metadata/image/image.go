package image

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	// Image decoders
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"

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

type imageextractor struct{}

type imagesummarizer struct {
	Title    string   `json:"title" min:"1" help:"A short, specific title for the image, in English."`
	Summary  string   `json:"summary" min:"1" help:"A one-paragraph summary of what the image depicts, in English."`
	Keywords []string `json:"keywords,omitempty" help:"Concepts, names, countries, regions and categories, only when there is a strong correlation to the image."`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	OllamaUrl   = "http://nestor.tailnet-db1f.ts.net:11434"
	OllamaModel = "qwen3.5"
)

var (
	ollamaMu     sync.Mutex
	ollamaClient *ollama.Client
	ollamaModel  *llmschema.Model
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(imageextractor))
}

func NewImageSummarizer(ctx context.Context) (*imagesummarizer, error) {
	ollamaMu.Lock()
	defer ollamaMu.Unlock()

	if ollamaClient == nil {
		c, err := ollama.New(OllamaUrl, client.OptTimeout(5*time.Minute))
		if err != nil {
			return nil, err
		}
		model, err := c.GetModel(ctx, OllamaModel)
		if err != nil {
			return nil, err
		}
		ollamaClient = c
		ollamaModel = model
	}
	return new(imagesummarizer), nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - EXTRACTOR

func (e *imageextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`^image/.*`)
}

func (e *imageextractor) ExtractMetadata(ctx context.Context, r io.ReadSeeker) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Read the exif metadata from the image
	exifMeta := ExtractEXIF(r)

	// Seek back to the beginning of the file
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	// Create artwork file, and return image metdata
	artwork, meta, err := CreateArtwork(r)
	if err != nil {
		return meta, []*schema.ArtworkMeta{artwork}, err
	}
	meta = append(meta, exifMeta...)

	// Now summarize the image
	summarizer, err := NewImageSummarizer(ctx)
	if err != nil {
		return meta, []*schema.ArtworkMeta{artwork}, err
	} else if err := summarizer.Summarize(ctx, artwork.Data, meta...); err != nil {
		return meta, []*schema.ArtworkMeta{artwork}, err
	}

	// Add the summary to the metadata
	meta = schema.AppendMeta(meta, metadata.ImageTitle, summarizer.Title)
	meta = schema.AppendMeta(meta, metadata.ImageSummary, summarizer.Summary)
	if len(summarizer.Keywords) > 0 {
		meta = schema.AppendMeta(meta, metadata.ImageTags, summarizer.Keywords)
	}

	// Return the metadata and artwork
	return meta, []*schema.ArtworkMeta{artwork}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - SUMMARIZER

const summarizeInstruction = "Summarize this image in English, with title and summary paragraph. Include concepts, names, countries, regions and categories as keywords when there is a strong correlation to those concepts in the image. The returned data should be in JSON format, with no markdown or other formatting."

func (r *imagesummarizer) Summarize(ctx context.Context, data []byte, meta ...schema.Meta) error {
	if ollamaClient == nil {
		return nil
	}

	message, err := llmschema.NewMessage(llmschema.RoleUser, summarizePrompt(meta), llmschema.WithAttachment(bytes.NewReader(data)))
	if err != nil {
		return err
	}

	opts := []llmopt.Opt{
		llmopt.SetBool(llmopt.ThinkingKey, false),
		ollama.WithJSONOutput(jsonschema.MustFor[imagesummarizer]()),
	}

	session := new(llmschema.Conversation)
	response, _, err := ollamaClient.WithSession(ctx, types.Value(ollamaModel), session, message, opts...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(stripCodeFence(response.Text())), r); err != nil {
		return err
	}

	// Return success
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// summarizePrompt builds the instruction text sent to the model, appending
// any known metadata (EXIF fields, dimensions, etc.) as extra context so the
// model can ground its summary in facts we already know rather than guessing.
func summarizePrompt(meta []schema.Meta) string {
	if len(meta) == 0 {
		return summarizeInstruction
	}

	var sb strings.Builder
	sb.WriteString(summarizeInstruction)
	sb.WriteString("\n\nThe following metadata was extracted from the image file and can be used as additional context:\n")
	for _, m := range meta {
		fmt.Fprintf(&sb, "- %s: %s\n", m.Key, metaValueString(m.Value))
	}
	return sb.String()
}

// metaValueString renders a Meta value for inclusion in a prompt, unquoting
// JSON string values so they read naturally as plain text.
func metaValueString(v json.RawMessage) string {
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s
	}
	return string(v)
}

// stripCodeFence removes a wrapping markdown code fence (e.g. "```json ...
// ```") from s, if present. Some models add one around JSON output despite
// being instructed not to.
func stripCodeFence(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimSuffix(s, "```")
	s = strings.TrimPrefix(s, "```")
	if i := strings.IndexByte(s, '\n'); i >= 0 && !strings.ContainsAny(s[:i], "{[\"") {
		s = s[i+1:]
	}
	return strings.TrimSpace(s)
}
