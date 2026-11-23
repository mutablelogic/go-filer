package filer

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Icon is an image which represents something, such as a backend: its media
// type, such as image/svg+xml, and its data
type Icon interface {
	// Type returns the media type of the image, such as image/svg+xml
	Type() string

	// Data returns the image
	Data() []byte
}
