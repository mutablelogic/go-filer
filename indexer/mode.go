package indexer

import (
	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Mode is what's indexed of a volume
type Mode string

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// IndexParam is the option of a volume's URL, of the registry, which is
	// what's indexed of it, such as index=folders
	IndexParam = "index"

	ModeNone     Mode = "none"     // It isn't indexed
	ModeFolders  Mode = "folders"  // Its folders are indexed
	ModeObjects  Mode = "objects"  // Its folders, and files, are indexed, with the backend's metadata
	ModeMetadata Mode = "metadata" // Its folders, and files, are indexed, and its files are enriched with their metadata, of a task (see WithMetadataTask)
	ModeArtwork  Mode = "artwork"  // As metadata, and the task finds its files' artwork too, such as an album's cover, or a thumbnail, which the store keeps
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ParseMode returns the mode of the index option, which is ModeNone when
// it's empty
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case "", ModeNone:
		return ModeNone, nil
	case ModeFolders, ModeObjects, ModeMetadata, ModeArtwork:
		return Mode(value), nil
	default:
		return ModeNone, gofiler.ErrBadParameter.Withf("%s: %q, which is %s, %s, %s, %s or %s", IndexParam, value, ModeNone, ModeFolders, ModeObjects, ModeMetadata, ModeArtwork)
	}
}

// Level returns how much the mode indexes, of each mode including what the
// one below it does: none is 0, folders 1, objects 2, metadata 3, and
// artwork 4
func (m Mode) Level() int {
	switch m {
	case ModeFolders:
		return 1
	case ModeObjects:
		return 2
	case ModeMetadata:
		return 3
	case ModeArtwork:
		return 4
	default:
		return 0
	}
}

// Files returns true when the mode indexes files, as well as folders: of
// objects, or more
func (m Mode) Files() bool {
	return m.Level() >= ModeObjects.Level()
}
