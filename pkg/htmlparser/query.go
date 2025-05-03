package htmlparser

import (
	"fmt"
	"strings"

	// Packages
	xpath "github.com/antchfx/xpath"
	html "golang.org/x/net/html"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type nodeNavigator struct {
	root, curr *html.Node
	attr       int
}

var _ xpath.NodeNavigator = &nodeNavigator{}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// QuerySelectorAll searches a root html.Node that matches the specified XPath selectors.
func QuerySelectorAll(root *html.Node, selector *xpath.Expr) []*html.Node {
	var elems []*html.Node
	t := selector.Select(createXPathNavigator(root))
	for t.MoveNext() {
		nav := t.Current().(*nodeNavigator)
		elems = append(elems, getCurrentNode(nav))
	}
	return elems
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// CreateXPathNavigator creates a new xpath.NodeNavigator for the specified html.Node.
func createXPathNavigator(top *html.Node) *nodeNavigator {
	return &nodeNavigator{curr: top, root: top, attr: -1}
}

func getCurrentNode(n *nodeNavigator) *html.Node {
	if n.NodeType() == xpath.AttributeNode {
		childNode := &html.Node{
			Type: html.TextNode,
			Data: n.Value(),
		}
		return &html.Node{
			Type:       html.ElementNode,
			Data:       n.LocalName(),
			FirstChild: childNode,
			LastChild:  childNode,
		}

	}
	return n.curr
}

// InnerText returns the text between the start and end tags of the object.
func innerText(n *html.Node) string {
	var output func(*strings.Builder, *html.Node)
	output = func(b *strings.Builder, n *html.Node) {
		switch n.Type {
		case html.TextNode:
			b.WriteString(n.Data)
			return
		case html.CommentNode:
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			output(b, child)
		}
	}

	var b strings.Builder
	output(&b, n)
	return b.String()
}

////////////////////////////////////////////////////////////////////////////////
// NODE NAVIGATOR METHODS

func (h *nodeNavigator) Current() *html.Node {
	return h.curr
}

func (h *nodeNavigator) NodeType() xpath.NodeType {
	switch h.curr.Type {
	case html.CommentNode:
		return xpath.CommentNode
	case html.TextNode:
		return xpath.TextNode
	case html.DocumentNode:
		return xpath.RootNode
	case html.ElementNode:
		if h.attr != -1 {
			return xpath.AttributeNode
		}
		return xpath.ElementNode
	case html.DoctypeNode:
		// ignored <!DOCTYPE HTML> declare and as Root-Node type.
		return xpath.RootNode
	}
	panic(fmt.Sprintf("unknown HTML node type: %v", h.curr.Type))
}

func (h *nodeNavigator) LocalName() string {
	if h.attr != -1 {
		return h.curr.Attr[h.attr].Key
	}
	return h.curr.Data
}

func (*nodeNavigator) Prefix() string {
	return ""
}

func (h *nodeNavigator) Value() string {
	switch h.curr.Type {
	case html.CommentNode:
		return h.curr.Data
	case html.ElementNode:
		if h.attr != -1 {
			return h.curr.Attr[h.attr].Val
		}
		return innerText(h.curr)
	case html.TextNode:
		return h.curr.Data
	}
	return ""
}

func (h *nodeNavigator) Copy() xpath.NodeNavigator {
	n := *h
	return &n
}

func (h *nodeNavigator) MoveToRoot() {
	h.curr = h.root
}

func (h *nodeNavigator) MoveToParent() bool {
	if h.attr != -1 {
		h.attr = -1
		return true
	} else if node := h.curr.Parent; node != nil {
		h.curr = node
		return true
	}
	return false
}

func (h *nodeNavigator) MoveToNextAttribute() bool {
	if h.attr >= len(h.curr.Attr)-1 {
		return false
	}
	h.attr++
	return true
}

func (h *nodeNavigator) MoveToChild() bool {
	if h.attr != -1 {
		return false
	}
	if node := h.curr.FirstChild; node != nil {
		h.curr = node
		return true
	}
	return false
}

func (h *nodeNavigator) MoveToFirst() bool {
	if h.attr != -1 || h.curr.PrevSibling == nil {
		return false
	}
	for {
		node := h.curr.PrevSibling
		if node == nil {
			break
		}
		h.curr = node
	}
	return true
}

func (h *nodeNavigator) String() string {
	return h.Value()
}

func (h *nodeNavigator) MoveToNext() bool {
	if h.attr != -1 {
		return false
	}
	if node := h.curr.NextSibling; node != nil {
		h.curr = node
		return true
	}
	return false
}

func (h *nodeNavigator) MoveToPrevious() bool {
	if h.attr != -1 {
		return false
	}
	if node := h.curr.PrevSibling; node != nil {
		h.curr = node
		return true
	}
	return false
}

func (h *nodeNavigator) MoveTo(other xpath.NodeNavigator) bool {
	node, ok := other.(*nodeNavigator)
	if !ok || node.root != h.root {
		return false
	}

	h.curr = node.curr
	h.attr = node.attr
	return true
}
