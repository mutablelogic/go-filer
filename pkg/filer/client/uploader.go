package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	// Packages
	client "github.com/mutablelogic/go-client"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Uploader interface {
	// Add one or more files to the request, with the given form name and
	// path. If the path is relative, it is changed to an absolute path.
	// If the path is a directory, all files under that path are uploaded
	// recursively.
	Add(name string, path ...string) error
}

type uploader struct {
	buf        *bytes.Buffer
	writer     *multipart.Writer
	opts       *opt
	cur, total uint64
}

var _ Uploader = (*uploader)(nil)
var _ io.Reader = (*uploader)(nil)
var _ client.Payload = (*uploader)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewUploader(opts ...Opt) *uploader {
	r := new(uploader)

	opt, err := applyOpts(opts...)
	if err != nil {
		return nil
	} else {
		r.opts = opt
	}
	r.buf = new(bytes.Buffer)
	r.writer = multipart.NewWriter(r.buf)

	// Return success
	return r
}

func (u *uploader) Close() error {
	return u.writer.Close()
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Return the method used for uploading the set of files
func (u *uploader) Method() string {
	return http.MethodPost
}

// Return the content type for the response
func (u *uploader) Accept() string {
	return ""
}

// Return the content type for the request
func (u *uploader) Type() string {
	return u.writer.FormDataContentType()
}

// Add one or more files to the request, with the given form name and
// path. If the path is relative, it is changed to an absolute path.
// If the path is a directory, all files under that path are uploaded
// recursively.
func (u *uploader) Add(name string, path ...string) error {
	// check form name
	if !types.IsIdentifier(name) {
		return fmt.Errorf("invalid form name: %q", name)
	}
	for _, path := range path {
		if err := u.add(name, path); err != nil {
			return err
		}
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (u *uploader) Read(b []byte) (int, error) {
	n, err := u.buf.Read(b)
	if u.opts.fn != nil && n > 0 {
		u.cur += uint64(n)
		u.opts.fn(u.cur, u.total)
	}
	return n, err
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (u *uploader) add(name string, path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("does not exist: %s", path)
	} else if err != nil {
		return err
	}

	// Walk the path, adding non-hidden regular files
	i := 0
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		// Check for errors
		if err != nil {
			return err
		}

		// Skip hidden files and directories
		if info.Name() != "." && strings.HasPrefix(info.Name(), ".") {
			return nil
		}

		// Recurse into directory
		if info.IsDir() {
			return nil
		}

		// Skip non-regular files
		if !info.Mode().IsRegular() {
			return nil
		}

		// Convert file into absolute path
		if path, err = filepath.Abs(path); err != nil {
			return err
		} else if err := u.addFile(fmt.Sprintf("%s%03d", name, i), path, info); err != nil {
			return err
		} else {
			u.total += uint64(info.Size())
			i++
		}

		// Return success
		return nil
	})

	return nil
}

func (u *uploader) addFile(name string, path string, info os.FileInfo) error {
	part, err := u.createFormFile(name, path, info)
	if err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// TODO: Use io.Pipe to stream the file
	if _, err := io.Copy(part, f); err != nil {
		return err
	}

	// Return success
	return nil
}

func (u *uploader) createFormFile(fieldname, path string, info os.FileInfo) (io.Writer, error) {
	var buf [1024]byte

	// Open file to detect mimetype
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read up to the first 1024 bytes
	n, err := f.Read(buf[:])
	if err != nil {
		return nil, err
	}

	// Set header fields
	h := make(textproto.MIMEHeader)
	h.Set(types.ContentDispositonHeader, fmt.Sprintf(`form-data; name=%v; filename=%v`, strconv.Quote(fieldname), strconv.Quote(path)))
	h.Set(types.ContentTypeHeader, http.DetectContentType(buf[:n]))
	h.Set(types.ContentLengthHeader, strconv.FormatInt(info.Size(), 10))
	h.Set(types.ContentModifiedHeader, info.ModTime().Format(http.TimeFormat))

	// Return the part
	return u.writer.CreatePart(h)
}
