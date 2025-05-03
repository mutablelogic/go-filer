package rss

import "encoding/xml"

const (
	NS_ITUNES = "http://www.itunes.com/dtds/podcast-1.0.dtd"
)

// ITunesFeedExtension is a set of extension fields for RSS feeds.
type ITunesFeedExtension struct {
	Author     string       `json:"author,omitempty" xml:"author"`
	Explicit   string       `json:"explicit,omitempty"  xml:"explicit"`
	Type       string       `json:"type,omitempty" xml:"type"`
	Owner      *ITunesOwner `json:"owner,omitempty" xml:"owner"`
	Block      string       `json:"block,omitempty" xml:"block"`
	Complete   string       `json:"complete,omitempty" xml:"complete"`
	NewFeedURL string       `json:"new-feed-url,omitempty" xml:"new-feed-url"`
}

// ITunesItemExtension is a set of extension fields for RSS items.
type ITunesItemExtension struct {
	Duration    *Duration `json:"duration,omitempty" xml:"duration"`
	Image       *Image    `json:"image,omitempty" xml:"image"`
	Explicit    string    `json:"explicit,omitempty" xml:"explicit"`
	Episode     string    `json:"episode,omitempty" xml:"episode"`
	Season      string    `json:"season,omitempty" xml:"season"`
	EpisodeType string    `json:"episodeType,omitempty" xml:"episodeType"`
	Block       string    `json:"block,omitempty" xml:"block"`
}

// ITunesOwner is the owner of a particular itunes feed.
type ITunesOwner struct {
	XMLName xml.Name `json:"-" xml:"owner"`
	Email   string   `json:"email,omitempty" xml:"email"`
	Name    string   `json:"name,omitempty" xml:"name"`
}
