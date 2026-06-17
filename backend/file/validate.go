package file

import (
	"net/url"
	"os"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
)

func Validate(url *url.URL) (string, error) {
	if url == nil || url.Scheme != "file" {
		return "", gofiler.ErrBadParameter.With("url with scheme 'file' is required")
	} else if name := url.Host; !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid file backend name: %q", name)
	} else if info, err := os.Stat(url.Path); err != nil {
		return "", gofiler.ErrBadParameter.Withf("invalid file backend path: %q", url.Path)
	} else if !info.IsDir() {
		return "", gofiler.ErrBadParameter.Withf("file backend path is not a directory: %q", url.Path)
	} else {
		return name, nil
	}
}
