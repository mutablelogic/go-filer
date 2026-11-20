package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// DriveWrite is the metadata of a file which is uploaded: its name, and
// folder, of one which is created, its type, and when it was modified
type DriveWrite struct {
	Name         string    `json:"name,omitempty"`
	Parents      []string  `json:"parents,omitempty"`
	MimeType     string    `json:"mimeType,omitempty"`
	ModifiedTime time.Time `json:"modifiedTime,omitzero"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// DefaultUploadChunk is how much of a file is uploaded in each request,
// which is all that's in memory of it, as it's uploaded, a multiple of
// 256 KiB, as Google's resumable uploads must be
const DefaultUploadChunk = 8 << 20

// uploadChunkUnit is what a chunk of an upload, apart from its last, is a
// multiple of
const uploadChunkUnit = 256 << 10

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// File returns a file, of its id, of the fields which are listed
func (c *DriveClient) File(ctx context.Context, id string) (*DriveFile, error) {
	var file DriveFile
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &file,
		client.OptPath("files", id),
		client.OptQuery(url.Values{"fields": {driveFileFields}}),
	); err != nil {
		return nil, err
	}
	return &file, nil
}

// CreateFile creates a file, without content, or a folder, of its type, of
// its metadata, which has its name, and the folder it's in, and returns it
func (c *DriveClient) CreateFile(ctx context.Context, meta DriveWrite) (*DriveFile, error) {
	req, err := client.NewJSONRequestEx(http.MethodPost, meta, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}
	var file DriveFile
	if err := c.DoAuthWithContext(ctx, req, &file,
		client.OptPath("files"),
		client.OptQuery(url.Values{"fields": {driveFileFields}}),
	); err != nil {
		return nil, err
	}
	return &file, nil
}

// Trash moves a file, or a folder, with what's in it, of its id, to the
// bin, from which it can be restored, and returns it
func (c *DriveClient) Trash(ctx context.Context, id string) (*DriveFile, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, map[string]bool{"trashed": true}, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}
	var file DriveFile
	if err := c.DoAuthWithContext(ctx, req, &file,
		client.OptPath("files", id),
		client.OptQuery(url.Values{"fields": {driveFileFields}}),
	); err != nil {
		return nil, err
	}
	return &file, nil
}

// Upload uploads a file's content, as a new file, of its metadata, or, of
// an id, as a new revision of that file, keeping its id, link, and who it's
// shared with, and returns it. The content is uploaded in chunks, of
// chunk, or DefaultUploadChunk, when it's zero, with Google's resumable
// upload, as it's read, so all of it isn't in memory, of requests which
// have the client's token, quota, and tracing, but no timeout, other than
// the context's.
// Reference: https://developers.google.com/drive/api/guides/manage-uploads#resumable
func (c *DriveClient) Upload(ctx context.Context, id string, meta DriveWrite, r io.Reader, chunk int) (*DriveFile, error) {
	if chunk <= 0 {
		chunk = DefaultUploadChunk
	} else if chunk%uploadChunkUnit != 0 {
		return nil, fmt.Errorf("upload: a chunk is a multiple of %d bytes, not %d", uploadChunkUnit, chunk)
	}
	session, err := c.uploadSession(ctx, id, meta)
	if err != nil {
		return nil, err
	}

	// Each chunk, which is the last, when it's short, so the size is known
	buf := make([]byte, chunk)
	var offset int64
	for {
		n, err := io.ReadFull(r, buf)
		last := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		if err != nil && !last {
			return nil, err
		}
		var contentRange string
		switch {
		case last && n == 0:
			contentRange = fmt.Sprintf("bytes */%d", offset)
		case last:
			contentRange = fmt.Sprintf("bytes %d-%d/%d", offset, offset+int64(n)-1, offset+int64(n))
		default:
			contentRange = fmt.Sprintf("bytes %d-%d/*", offset, offset+int64(n)-1)
		}
		file, err := c.uploadChunk(ctx, session, buf[:n], contentRange, offset+int64(n))
		if err != nil {
			return nil, err
		} else if file != nil {
			return file, nil
		} else if last {
			return nil, errors.New("upload: the last chunk wasn't accepted as the end of the file")
		}
		offset += int64(n)
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// uploadSession starts an upload, of a new file, or of a new revision of
// the file of the id, and returns the URL its chunks are uploaded to, which
// must be of the API, as its requests have the token
func (c *DriveClient) uploadSession(ctx context.Context, id string, meta DriveWrite) (string, error) {
	endpoint, err := c.uploadEndpoint(id)
	if err != nil {
		return "", err
	}
	method := http.MethodPost
	if id != "" {
		method = http.MethodPatch
	}
	req, err := client.NewJSONRequestEx(method, meta, types.ContentTypeAny)
	if err != nil {
		return "", err
	}
	opts := []client.RequestOpt{client.OptReqEndpoint(endpoint)}
	if meta.MimeType != "" {
		opts = append(opts, client.OptReqHeader("X-Upload-Content-Type", meta.MimeType))
	}
	var session string
	opts = append(opts, client.OptResponse(func(response *http.Response) error {
		if response.StatusCode == http.StatusOK {
			session = response.Header.Get("Location")
			return client.ErrResponseHandled
		}
		return nil
	}))
	if err := c.DoAuthWithContext(ctx, req, nil, opts...); err != nil {
		return "", err
	} else if session == "" {
		return "", errors.New("upload: no session was returned")
	}
	if u, err := url.Parse(session); err != nil {
		return "", err
	} else if api, err := url.Parse(endpoint); err != nil {
		return "", err
	} else if u.Scheme != api.Scheme || u.Host != api.Host {
		return "", fmt.Errorf("upload: the session isn't of %s", api.Host)
	}
	return session, nil
}

// uploadChunk uploads a chunk of a file, of its range, and returns the file,
// once it's uploaded, or nil, when Google has all of it until the end of
// the chunk, which isn't the last, which it says of a 308, Resume
// Incomplete, of its Range
func (c *DriveClient) uploadChunk(ctx context.Context, session string, chunk []byte, contentRange string, end int64) (*DriveFile, error) {
	var file DriveFile
	incomplete := false
	if err := c.DoAuthWithContext(ctx, client.NewBytesRequest(http.MethodPut, chunk, "", types.ContentTypeAny), &file,
		client.OptReqEndpoint(session),
		client.OptReqHeader("Content-Range", contentRange),
		client.OptNoTimeout(),
		client.OptResponse(func(response *http.Response) error {
			if response.StatusCode != http.StatusPermanentRedirect {
				return nil
			} else if got := response.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", end-1) {
				// What Google has isn't all of it, so far, as a chunk which is
				// short isn't resent
				return fmt.Errorf("upload: Google has %q of the file, not its first %d bytes", got, end)
			}
			incomplete = true
			return client.ErrResponseHandled
		}),
	); err != nil {
		return nil, err
	} else if incomplete {
		return nil, nil
	}
	return &file, nil
}

// uploadEndpoint returns the URL of an upload, of a new file, or of the
// file of the id, which is the API's, beneath /upload, such as
// https://www.googleapis.com/upload/drive/v3/files, with the fields of the
// file which is returned
func (c *DriveClient) uploadEndpoint(id string) (string, error) {
	u, err := url.Parse(c.Endpoint())
	if err != nil {
		return "", err
	}
	u.Path = "/upload" + strings.TrimSuffix(u.Path, "/") + "/files"
	if id != "" {
		u.Path += "/" + url.PathEscape(id)
	}
	u.RawPath = ""
	u.RawQuery = url.Values{"uploadType": {"resumable"}, "fields": {driveFileFields}}.Encode()
	return u.String(), nil
}

// parseVersion returns a file's version, as a number, or -1 when it has
// none, or it isn't one
func parseVersion(version string) int64 {
	if v, err := strconv.ParseInt(version, 10, 64); err == nil {
		return v
	}
	return -1
}

// VersionNumber returns a file's version, as a number, or -1 when it has
// none, or it isn't one
func (f *DriveFile) VersionNumber() int64 {
	return parseVersion(f.Version)
}

// NotOlder returns true when a file's version is the same as another's, or after it, or it can't
// be said, as either has no version
func (f *DriveFile) NotOlder(than *DriveFile) bool {
	a, b := parseVersion(f.Version), parseVersion(than.Version)
	return a < 0 || b < 0 || a >= b
}
