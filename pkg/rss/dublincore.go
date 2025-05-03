package rss

// DublinCoreExtension represents a feed extension for the Dublin Core specification.
type DublinCoreExtension struct {
	Title       []string `json:"title,omitempty"`
	Creator     []string `json:"creator,omitempty"`
	Author      []string `json:"author,omitempty"`
	Subject     []string `json:"subject,omitempty"`
	Description []string `json:"description,omitempty"`
	Publisher   []string `json:"publisher,omitempty"`
	Contributor []string `json:"contributor,omitempty"`
	Date        []string `json:"date,omitempty"`
	Type        []string `json:"type,omitempty"`
	Format      []string `json:"format,omitempty"`
	Identifier  []string `json:"identifier,omitempty"`
	Source      []string `json:"source,omitempty"`
	Language    []string `json:"language,omitempty"`
	Relation    []string `json:"relation,omitempty"`
	Coverage    []string `json:"coverage,omitempty"`
	Rights      []string `json:"rights,omitempty"`
}
