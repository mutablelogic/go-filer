package rss

import (
	"encoding/json"
	"encoding/xml"
)

// An RSS feed structure
type Feed struct {
	XMLName xml.Name `json:"-" xml:"rss"`
	Version string   `json:"version" xml:"version,attr"`
	Channel *Channel `json:"channel,omitempty" xml:"channel"`
}

type Channel struct {
	XMLName        xml.Name    `json:"-" xml:"channel"`
	Title          string      `json:"title,omitempty" xml:"title"`
	Link           string      `json:"link,omitempty" xml:"link"`
	Categories     []*Category `json:"category,omitempty"  xml:"category"`
	Cloud          *Cloud      `json:"cloud,omitempty" xml:"cloud"`
	Copyright      string      `json:"copyright,omitempty" xml:"copyright"`
	Description    string      `json:"description,omitempty" xml:"description"`
	Docs           string      `json:"docs,omitempty" xml:"docs"`
	Generator      string      `json:"generator,omitempty" xml:"generator"`
	Image          *Image      `json:"image,omitempty" xml:"image"`
	Items          []*Item     `json:"item,omitempty" xml:"item,omitempty"`
	Language       string      `json:"language,omitempty" xml:"language"`
	ManagingEditor string      `json:"managingEditor,omitempty" xml:"managingEditor"`
	PubDate        *Date       `json:"pubDate,omitempty" xml:"pubDate"`
	LastBuildDate  *Date       `json:"lastBuildDate,omitempty" xml:"lastBuildDate"`
	Rating         string      `json:"rating,omitempty" xml:"rating"`
	SkipDays       *Skip       `json:"skipDays,omitempty"  xml:"skipDays"`
	SkipHours      *Skip       `json:"skipHours,omitempty" xml:"skipHours"`
	TextInput      *TextInput  `json:"textInput,omitempty" xml:"textInput"`
	TTL            *Duration   `json:"ttl,omitempty" xml:"ttl"`
	WebMaster      string      `json:"webMaster,omitempty"  xml:"webMaster"`
	ITunesFeedExtension
}

// Item is an RSS Item
type Item struct {
	XMLName     xml.Name    `json:"-" xml:"item"`
	Author      string      `json:"author,omitempty" xml:"author"`
	Categories  []*Category `json:"category,omitempty" xml:"category"`
	Comments    string      `json:"comments,omitempty" xml:"comments"`
	Description string      `json:"description,omitempty" xml:"description"`
	GUID        *GUID       `json:"guid,omitempty" xml:"guid"`
	PubDate     *Date       `json:"pubDate,omitempty"  xml:"pubDate"`
	Source      *Source     `json:"source,omitempty" xml:"source"`
	Title       string      `json:"title,omitempty" xml:"title"`
	Link        []*Link     `json:"link,omitempty" xml:"link"`
	Enclosure   *Enclosure  `json:"enclosure,omitempty" xml:"enclosure"`
	ITunesItemExtension
}

// Date is an RSS Date
type Date struct {
	Value string `json:"value,omitempty" xml:",chardata"`
}

// Duration is a duration
type Duration struct {
	Value string `json:"value,omitempty" xml:",chardata"`
}

// Skip is an array of days or hours
type Skip struct {
	Day  []string `json:"day,omitempty" xml:"day"`
	Hour []string `json:"hour,omitempty" xml:"hour"`
}

// Link
type Link struct {
	XMLName xml.Name `json:"-" xml:"link"`
	URL     string   `json:"href,omitempty" xml:"href,attr"`
	Value   string   `json:"value,omitempty" xml:",chardata"`
}

// Image is an image that represents the feed
type Image struct {
	XMLName     xml.Name `json:"-" xml:"image"`
	URL         string   `json:"url,omitempty" xml:"url"`
	HRef        string   `json:"href,omitempty" xml:"href,attr"`
	Link        string   `json:"link,omitempty" xml:"link"`
	Title       string   `json:"title,omitempty" xml:"title"`
	Width       string   `json:"width,omitempty" xml:"width"`
	Height      string   `json:"height,omitempty" xml:"height"`
	Description string   `json:"description,omitempty" xml:"description"`
}

// Enclosure is a media object that is attached to
// the item
type Enclosure struct {
	XMLName xml.Name `json:"-" xml:"enclosure"`
	URL     string   `json:"url,omitempty" xml:"url,attr"`
	Length  string   `json:"length,omitempty" xml:"length,attr"`
	Type    string   `json:"type,omitempty" xml:"type,attr"`
	Value   string   `json:"value,omitempty" xml:",chardata"`
}

// GUID is a unique identifier for an item
type GUID struct {
	XMLName     xml.Name `json:"-" xml:"guid"`
	IsPermalink string   `json:"isPermalink,omitempty" xml:"isPermaLink,attr"`
	Value       string   `json:"value,omitempty" xml:",chardata"`
}

// Source contains feed information for another feed if a given item came from that feed
type Source struct {
	XMLName xml.Name `json:"-" xml:"source"`
	URL     string   `json:"url,omitempty" xml:"url,attr"`
	Value   string   `json:"value,omitempty" xml:",chardata"`
}

// Category is category metadata for Feeds and Entries
type Category struct {
	XMLName  xml.Name  `json:"-" xml:"category"`
	Domain   string    `json:"domain,omitempty" xml:"domain,attr"`
	Value    string    `json:"value,omitempty" xml:",chardata"`
	Text     string    `json:"text,omitempty" xml:"text,attr"`
	Category *Category `json:"category,omitempty" xml:"category"`
}

// TextInput specifies a text input box that
// can be displayed with the channel
type TextInput struct {
	XMLName     xml.Name `json:"-" xml:"textInput"`
	Title       string   `json:"title,omitempty" xml:"title"`
	Description string   `json:"description,omitempty"  xml:"description"`
	Name        string   `json:"name,omitempty"  xml:"name"`
	Link        string   `json:"link,omitempty"  xml:"link"`
}

// Cloud allows processes to register with a cloud to be notified of updates to the channel,
// implementing a lightweight publish-subscribe protocol for RSS feeds
type Cloud struct {
	Domain            string `json:"domain,omitempty" xml:"domain,attr"`
	Port              string `json:"port,omitempty" xml:"port,attr"`
	Path              string `json:"path,omitempty" xml:"path,attr"`
	RegisterProcedure string `json:"registerProcedure,omitempty" xml:"registerProcedure,attr"`
	Protocol          string `json:"protocol,omitempty" xml:"protocol,attr"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (f Feed) String() string {
	json, _ := json.MarshalIndent(f, "", "    ")
	return string(json)
}
