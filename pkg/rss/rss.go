package rss

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
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

// Reads the feed from an io.Reader
func (feed *Feed) Unmarshal(header http.Header, r io.Reader) error {
	mimetype, err := types.ParseContentType(header.Get(types.ContentTypeHeader))
	if err != nil {
		return err
	}
	switch mimetype {
	case types.ContentTypeXML, types.ContentTypeRSS:
		// Create an XML Parser
		if err := xml.NewDecoder(r).Decode(feed); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported mimetype: %q", mimetype)
	}

	// Return success
	return nil
}
