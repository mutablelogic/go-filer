package htmlparser

import (
	"fmt"
	"strings"

	// Packages
	"golang.org/x/net/html"
)

////////////////////////////////////////////////////////////////////////////
// TYPES

type Node struct {
	*html.Node
}

////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (n Node) String() string {
	switch n.Type {
	case html.ErrorNode:
		return fmt.Sprintf("error: %v", n.Data)
	case html.CommentNode:
		return fmt.Sprintf("<!-- %v -->", n.Data)
	case html.DoctypeNode:
		return fmt.Sprintf("<!DOCTYPE %v>", n.Data)
	case html.ElementNode:
		var attrs []string
		for _, attr := range n.Attr {
			attrs = append(attrs, fmt.Sprintf("%v=%q", attr.Key, attr.Val))
		}
		return fmt.Sprintf("<%v %v>", n.Data, strings.Join(attrs, " "))
	case html.TextNode:
		return n.Data
	}
	panic(fmt.Sprintf("Don't know how to stringify %v", n.Type))
}

////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (n Node) GetAttr(key string) string {
	for _, attr := range n.Attr {
		if attr.Key == key {
			return attr.Val
		}
	}
	return ""
}
