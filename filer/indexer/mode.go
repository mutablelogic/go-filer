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

	ModeNone    Mode = "none"    // It isn't indexed
	ModeFolders Mode = "folders" // Its folders are indexed
	ModeObjects Mode = "objects" // Its folders, and files, are indexed, with the backend's metadata
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ParseMode returns the mode of the index option, which is ModeNone when
// it's empty
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case "", ModeNone:
		return ModeNone, nil
	case ModeFolders, ModeObjects:
		return Mode(value), nil
	default:
		return ModeNone, gofiler.ErrBadParameter.Withf("%s: %q, which is %s, %s or %s", IndexParam, value, ModeNone, ModeFolders, ModeObjects)
	}
}

// Level returns how much the mode indexes, of each mode including what the
// one below it does: none is 0, folders 1, and objects 2
func (m Mode) Level() int {
	switch m {
	case ModeFolders:
		return 1
	case ModeObjects:
		return 2
	default:
		return 0
	}
}
