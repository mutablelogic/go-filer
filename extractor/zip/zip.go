package zip

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"time"

	// Packages
	registry "github.com/mutablelogic/go-filer/extractor/registry"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type extractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(extractor{})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e extractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`application/zip`)
}

func (e extractor) Extract(ctx context.Context, r io.ReaderAt, _ url.Values) error {
	size, err := zipSize(r)
	if err != nil {
		return err
	}

	zr, err := zip.NewReader(r, size)
	if err != nil {
		return err
	}

	for _, f := range zr.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		fmt.Printf("\tFile: %s, Method: %d, UncompressedSize64: %d, CompressedSize64: %d, ModTime: %s\n",
			f.Name, f.Method, f.UncompressedSize64, f.CompressedSize64, f.Modified.Format(time.Kitchen))
	}

	return nil
}

func zipSize(r io.ReaderAt) (int64, error) {
	type stater interface {
		Stat() (os.FileInfo, error)
	}
	if s, ok := r.(stater); ok {
		info, err := s.Stat()
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}
	return 0, io.ErrUnexpectedEOF
}
