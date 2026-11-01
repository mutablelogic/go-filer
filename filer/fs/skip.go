package fs

import (
	"strings"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// defaultSkip are the regular expressions of the names of files and
// directories which aren't listed, when the skip option isn't set, which are
// the patterns of the older filesystem, as they aren't
// content: hidden files, and the files which operating systems and
// applications leave alongside content, which a sync, such as Google
// Drive's, also uploads. The names of Windows files vary in case, so they're
// matched without it.
var defaultSkip = []string{
	`^\.`,                  // Hidden files and directories, such as iCloud's .name.icloud placeholders
	`^~\$`,                 // Microsoft Office owner files, while a document is open
	`~$`,                   // Backup files, which editors leave
	`(?i)^icon\r$`,         // macOS custom folder icon, whose name ends with a carriage return
	`(?i)^thumbs\.db$`,     // Windows thumbnail cache
	`(?i)^desktop\.ini$`,   // Windows folder settings
	`(?i)^\$recycle\.bin$`, // Windows recycle bin
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// skip returns true when a file or directory with the name isn't listed,
// nor anything in a directory with the name
func (self *FS) skip(name string) bool {
	for _, re := range self.opt.skip {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// skipPath returns true when an element of the path, separated by "/", is
// skipped, so a file at the path isn't listed. The "." and ".." elements
// aren't names, so they aren't skipped, and a path with them is invalid.
func (self *FS) skipPath(name string) bool {
	for elem := range strings.SplitSeq(name, "/") {
		if elem != "." && elem != ".." && self.skip(elem) {
			return true
		}
	}
	return false
}
