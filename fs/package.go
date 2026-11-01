package fs

import (
	"mime"
	"path"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// defaultPackage are the regular expressions of the names of the macOS
// packages, when the package option isn't set, which are the patterns of the
// older filesystem: directories which are shown as one file, as Finder shows
// them, such as a sparse disk image, whose bands are each a piece of a disk,
// which would otherwise be typed by whatever they start with. A package is
// listed as a file, which can't be read, and the files in it aren't listed.
var defaultPackage = []string{
	`(?i)\.bundle$`,       // Bundles, such as plugins
	`(?i)\.sparsebundle$`, // Sparse disk images
}

// packageTypes are the content types of the packages, by their extensions
var packageTypes = map[string]string{
	".bundle":       "application/x-apple-bundle",
	".sparsebundle": "application/x-apple-sparsebundle",
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	// A package's type is its extension's
	for ext, contentType := range packageTypes {
		if err := mime.AddExtensionType(ext, contentType); err != nil {
			panic(err)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// isPackage returns true when a directory with the name is a package
func (self *FS) isPackage(name string) bool {
	for _, re := range self.opt.packages {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// inPackage returns true when the path, separated by "/", is in a package,
// which is when a directory it's in is one
func (self *FS) inPackage(name string) bool {
	_, ok := self.packageOf(name)
	return ok
}

// packageOf returns the package which the path, separated by "/", is in, or
// false when it isn't in one
func (self *FS) packageOf(name string) (string, bool) {
	elems := strings.Split(name, "/")
	for i, elem := range elems[:len(elems)-1] {
		if self.isPackage(elem) {
			return path.Join(elems[:i+1]...), true
		}
	}
	return "", false
}
