package pdf

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"

	// Packages
	reader "github.com/carlos7ags/folio/reader"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	text "github.com/mutablelogic/go-filer/metadata/text"
	llm "github.com/mutablelogic/go-llm"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type pdfextractor struct {
	client llm.Generator
	model  *llmschema.Model
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	maxTextSize = 64 * 1024
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(pdfextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *pdfextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`application/pdf`)
}

// Extract metadata from the file at the given path
func (e *pdfextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	// Initialise summarizer first so ollamaMaxInputTokens is set before reading
	summarizer, err := text.NewTextSummarizer(ctx)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pdf, err := reader.Parse(data)
	if err != nil {
		return nil, err
	}
	title, author, subject, creator, producer := pdf.Info()

	var kv []schema.MetadataKV
	kv = appendMetadata(kv, metadata.PDFTitle, title)
	kv = appendMetadata(kv, metadata.PDFAuthor, author)
	kv = appendMetadata(kv, metadata.PDFSubject, subject)
	kv = appendMetadata(kv, metadata.PDFCreator, creator)
	kv = appendMetadata(kv, metadata.PDFProducer, producer)
	kv = appendMetadata(kv, metadata.PDFPages, pdf.PageCount())

	// Summarize the PDF by collecting page text up to 64K.
	var builder strings.Builder
	for i := 0; i < pdf.PageCount() && builder.Len() < maxTextSize; i++ {
		if err := ctx.Err(); err != nil {
			return kv, err
		}

		page, err := pdf.Page(i)
		if err != nil {
			continue
		}

		pageText, err := page.ExtractText()
		if err != nil || strings.TrimSpace(pageText) == "" {
			continue
		}

		remaining := maxTextSize - builder.Len()
		if len(pageText) > remaining {
			pageText = pageText[:remaining]
		}
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(pageText)
	}

	// Now summarize the text
	if kv_, err := summarizer.Summarize(ctx, builder.String()); err != nil {
		return kv, err
	} else if len(kv_) > 0 {
		kv = append(kv, kv_...)
	}

	return kv, nil
}

func appendMetadata(kv []schema.MetadataKV, key string, value any) []schema.MetadataKV {
	value = normalizeMetadataValue(value)

	var jsonValue json.RawMessage
	b, err := json.Marshal(value)
	if err != nil {
		return kv
	}

	jsonValue = json.RawMessage(b)
	if string(jsonValue) == "null" || string(jsonValue) == `""` {
		return kv
	}
	return append(kv, schema.MetadataKV{Key: key, Value: jsonValue})
}

func normalizeMetadataValue(value any) any {
	switch v := value.(type) {
	case string:
		return sanitizeUnicode(v)
	case []byte:
		return sanitizeUnicode(string(v))
	default:
		return value
	}
}

func sanitizeUnicode(s string) string {
	// Ensure valid UTF-8 and remove NUL bytes, which PostgreSQL JSONB rejects.
	b := bytes.ToValidUTF8([]byte(s), []byte(""))
	b = bytes.ReplaceAll(b, []byte{0}, nil)
	return string(b)
}

/*

func (e *pdfextractor) Extract(ctx context.Context, r io.ReaderAt, _ url.Values) error {
	data, err := readAllReaderAt(r)
	if err != nil {
		return err
	}

	pdf, err := reader.Parse(data)
	if err != nil {
		return err
	}

	title, author, subject, creator, producer := pdf.Info()
	fmt.Printf("PDF: title=%s author=%s subject=%s creator=%s producer=%s pages=%v\n", title, author, subject, creator, producer, pdf.PageCount())

	for i := 0; i < pdf.PageCount(); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := pdf.Page(i)
		if err != nil {
			return err
		}
		text, err := page.ExtractText()
		if err != nil {
			return err
		}

		// We split the text into chunks to avoid hitting the token limit of the model.
		// We could also use a sliding window approach to get more context, but for simplicity we just split it into chunks.
		for len(text) > 80 {
			chunk := text
			if len(chunk) > chunkSize {
				chunk = chunk[:chunkSize]
			}
			if err := e.summarize(ctx, page.Number, chunk); err != nil {
				return err
			}
			text = text[len(chunk):]
		}
	}

	return nil
}

func readAllReaderAt(r io.ReaderAt) ([]byte, error) {
	type stater interface {
		Stat() (os.FileInfo, error)
	}

	s, ok := r.(stater)
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}

	info, err := s.Stat()
	if err != nil {
		return nil, err
	}

	return io.ReadAll(io.NewSectionReader(r, 0, info.Size()))
}

func (e *extractor) summarize(ctx context.Context, page int, text string) error {
	if e.client == nil {
		if client, err := ollama.New("http://nestor.local:11434", client.OptTimeout(5*time.Minute)); err != nil {
			return err
		} else if model, err := client.GetModel(ctx, "phi4"); err != nil {
			return err
		} else {
			e.model = model
			e.client = client
		}
	}

	type output struct {
		Title    string
		Summary  string
		Keywords []string
	}
	type outputpage struct {
		Page int
		output
	}

	opts := []llmopt.Opt{
		llmopt.AddString(llmopt.SystemPromptKey, "Summarize the purpose of the following document into a short paragraph in English, with title, summary paragraph. Include concepts, names, countries, regions and categories as keywords when the text is substantive about those concepts."),
		ollama.WithJSONOutput(jsonschema.MustFor[output]()),
	}
	message, err := llmschema.NewMessage(llmschema.RoleUser, text)
	if err != nil {
		return err
	}

	response, _, err := e.client.WithoutSession(ctx, types.Value(e.model), message, opts...)
	if err != nil {
		return err
	}

	var outputData outputpage
	if err := json.Unmarshal([]byte(response.Text()), &outputData); err != nil {
		return err
	} else {
		outputData.Page = page
	}

	pretty, err := json.MarshalIndent(outputData, "\t", "  ")
	if err != nil {
		return err
	}

	fmt.Println("\t", string(pretty))

	return nil
}
*/
