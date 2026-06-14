package registry

import (
	"fmt"
	"regexp"

	// Packages
	extractor "github.com/mutablelogic/go-filer/extractor"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	extractors = make(map[*regexp.Regexp]extractor.Extractor)
)

func RegisterExtractor(e extractor.Extractor) {
	extractors[e.MediaType()] = e
}

func Get(mimeType string) (extractor.Extractor, error) {
	for re, e := range extractors {
		if !re.MatchString(mimeType) {
			continue
		}
		return e, nil
	}
	return nil, fmt.Errorf("no extractor registered for media type %s", mimeType)
}
