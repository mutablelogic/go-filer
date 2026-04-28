package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	// Packages

	"github.com/mutablelogic/go-llm/kernel/schema"
	"github.com/mutablelogic/go-llm/provider/google"
	"github.com/mutablelogic/go-server/pkg/jsonschema"
	"github.com/mutablelogic/go-server/pkg/types"
)

type Summary struct {
	Title   string   `json:"title" help:"Extracted or synthesized title of the content." required:""`
	Summary string   `json:"summary" help:"Summary of the content which explains the main points and concepts in markdown format." required:""`
	Tags    []string `json:"tags" help:"Comma separated list of tags, such as category of content, place names, authors, programming languages, etc." required:""`
}

const (
	model = "gemini-3.1-flash-lite-preview"
)

var (
	outputFormat = jsonschema.MustFor[Summary]()
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Read input from files
	var data []byte
	for _, file := range os.Args[1:] {
		input, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", file, err)
			return
		}
		data = append(data, input...)
	}

	// Create client
	llm, err := google.New(os.Getenv("GEMINI_API_KEY"))
	if err != nil {
		fmt.Println("Error creating LLM client:", err)
		return
	}

	// Get the model
	model, err := llm.GetModel(ctx, model)
	if err != nil {
		fmt.Println("Error getting model:", err)
		return
	}

	// Message
	message, err := schema.NewMessage(schema.RoleUser, string(data))
	if err != nil {
		fmt.Println("Error creating message:", err)
		return
	}

	// Complete a message
	response, _, err := llm.WithoutSession(ctx, types.Value(model), message, google.WithJSONOutput(outputFormat))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	var out Summary
	err = json.Unmarshal([]byte(response.Text()), &out)
	if err != nil {
		fmt.Println("Error marshaling response:", err)
		return
	}
	fmt.Println(types.Stringify(out))
}
