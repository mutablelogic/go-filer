package htmlparser

import (
	"errors"
	"io"

	// Packages
	xpath "github.com/antchfx/xpath"
	html "golang.org/x/net/html"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Parser struct {
	Rules []Rule
}

type Rule struct {
	Query   *xpath.Expr
	Handler func(Node) error
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Create a new parser with a specific URL, which "hooks" a specific set
// of rules into the parser
func New() *Parser {
	self := new(Parser)
	self.Rules = make([]Rule, 0, 10)
	return self
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (p *Parser) AddRule(rule string, handler func(Node) error) error {
	if query, err := xpath.Compile(rule); err != nil {
		return err
	} else {
		p.Rules = append(p.Rules, Rule{query, handler})
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (p *Parser) Read(r io.Reader) error {
	var result error

	doc, err := html.Parse(r)
	if err != nil {
		return err
	}

	// Apply all the rules
	for _, rule := range p.Rules {
		nodes := QuerySelectorAll(doc, rule.Query)
		for _, node := range nodes {
			result = errors.Join(result, rule.Handler(Node{node}))
		}
	}

	// Return any errors
	return result
}
