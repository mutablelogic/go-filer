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
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ParseMode returns the mode of the index option, which is ModeNone when
// it's empty
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case "", ModeNone:
		return ModeNone, nil
	case ModeFolders:
		return ModeFolders, nil
	default:
		return ModeNone, gofiler.ErrBadParameter.Withf("%s: %q, which is %s or %s", IndexParam, value, ModeNone, ModeFolders)
	}
}
