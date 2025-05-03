package rss

import (
	"encoding/xml"
	"io"
)

// Create an RSS feed from an io.Reader
func Read(r io.Reader) (*Feed, error) {
	var feed Feed
	// Create an XML Parser
	if err := xml.NewDecoder(r).Decode(&feed); err != nil {
		return nil, err
	} else {
		return &feed, nil
	}
}
