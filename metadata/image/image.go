package image

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"regexp"
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
	Title    string
	Summary  string
	Keywords []string
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

func (e *imageextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Create artwork file, and return image metdata
	artwork, meta, err := CreateArtwork(r)
	if err != nil {
		return meta, []*schema.ArtworkMeta{artwork}, err
	}

	// Now summarize the image
	summarizer, err := NewImageSummarizer(ctx)
	if err != nil {
		return meta, []*schema.ArtworkMeta{artwork}, err
	} else if err := summarizer.Summarize(ctx, artwork.Data); err != nil {
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

func (r *imagesummarizer) Summarize(ctx context.Context, data []byte) error {
	if ollamaClient == nil {
		return nil
	}
	opts := []llmopt.Opt{
		llmopt.SetBool(llmopt.ThinkingKey, false),
		ollama.WithJSONOutput(jsonschema.MustFor[imagesummarizer]()),
	}

	file, err := os.CreateTemp("", "go-filer-image-*.bin")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	message, err := llmschema.NewMessage(llmschema.RoleUser, "Summarize this image in English, with title and summary paragraph. Include concepts, names, countries, regions and categories as keywords when there is a strong correlationto those concepts in the image. The returned data should be in JSON format, with no markdown or other formatting.", llmschema.WithAttachment(file))
	if err != nil {
		return err
	}

	session := new(llmschema.Conversation)
	response, _, err := ollamaClient.WithSession(ctx, types.Value(ollamaModel), session, message, opts...)
	if err != nil {
		return err
	} else if err := json.Unmarshal([]byte(response.Text()), r); err != nil {
		return err
	}

	// Return success
	return nil
}
