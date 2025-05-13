package aws

import (
	"context"

	// Packages
	llm "github.com/mutablelogic/go-llm"
	agent "github.com/mutablelogic/go-llm/pkg/agent"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Config struct {
	GeminiAPIKey    string `name:"gemini-api-key" env:"GEMINI_API_KEY" help:"Gemini API key"`
	AnthropicAPIKey string `name:"anthropic-api-key" env:"ANTHROPIC_API_KEY" help:"Anthropic API key"`
	MistralAPIKey   string `name:"mistral-api-key" env:"MISTRAL_API_KEY" help:"Mistral API key"`
	OpenAIAPIKey    string `name:"openai-api-key" env:"OPENAI_API_KEY" help:"OpenAI API key"`
	OllamaUrl       string `name:"ollama-url" env:"OLLAMA_URL" help:"Ollama URL"`
}

type task struct {
	client llm.Agent
}

var _ server.Plugin = Config{}
var _ server.Task = task{}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (c Config) New(ctx context.Context) (server.Task, error) {
	task := new(task)

	// Set agents
	opts := []llm.Opt{}
	if c.GeminiAPIKey != "" {
		opts = append(opts, agent.WithGemini(c.GeminiAPIKey))
	}
	if c.AnthropicAPIKey != "" {
		opts = append(opts, agent.WithAnthropic(c.AnthropicAPIKey))
	}
	if c.MistralAPIKey != "" {
		opts = append(opts, agent.WithMistral(c.MistralAPIKey))
	}
	if c.OpenAIAPIKey != "" {
		opts = append(opts, agent.WithOpenAI(c.OpenAIAPIKey))
	}
	if c.OllamaUrl != "" {
		opts = append(opts, agent.WithOllama(c.OllamaUrl))
	}

	// Create a new client
	agent, err := agent.New(opts...)
	if err != nil {
		return nil, err
	} else {
		task.client = agent
	}

	// Return an AWS task
	return task, nil
}

////////////////////////////////////////////////////////////////////////////////
// MODULE

func (Config) Name() string {
	return "llm"
}

func (Config) Description() string {
	return "LLM services"
}

////////////////////////////////////////////////////////////////////////////////
// MODULE

func (task) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
