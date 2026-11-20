package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
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
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
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

// CreateFolder creates a folder, of its name, in the folder of the parent's
// id, and returns it
func (c *DriveClient) CreateFolder(ctx context.Context, name, parent string) (*DriveFile, error) {
	req, err := client.NewJSONRequestEx(http.MethodPost, DriveWrite{Name: name, Parents: []string{parent}, MimeType: DriveFolderType}, types.ContentTypeJSON)
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
// the file of the id, and returns the URL its chunks are uploaded to
func (c *DriveClient) uploadSession(ctx context.Context, id string, meta DriveWrite) (string, error) {
	endpoint, err := c.uploadEndpoint(id)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	method := http.MethodPost
	if id != "" {
		method = http.MethodPatch
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", types.ContentTypeJSON+"; charset=UTF-8")
	if meta.MimeType != "" {
		req.Header.Set("X-Upload-Content-Type", meta.MimeType)
	}
	response, err := c.raw().Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if err := statusError(response); err != nil {
		return "", err
	}
	session := response.Header.Get("Location")
	if session == "" {
		return "", errors.New("upload: no session was returned")
	}

	// The session must be of the API, as its requests have the token
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
// the chunk, which isn't the last
func (c *DriveClient) uploadChunk(ctx context.Context, session string, chunk []byte, contentRange string, end int64) (*DriveFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, session, bytes.NewReader(chunk))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(chunk))
	req.Header.Set("Content-Range", contentRange)
	response, err := c.raw().Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusPermanentRedirect {
		// Resume Incomplete, of what Google has, which is all of it, so far,
		// as a chunk which is short isn't resent
		if got := response.Header.Get("Range"); got != fmt.Sprintf("bytes=0-%d", end-1) {
			return nil, fmt.Errorf("upload: Google has %q of the file, not its first %d bytes", got, end)
		}
		return nil, nil
	} else if err := statusError(response); err != nil {
		return nil, err
	}
	var file DriveFile
	if err := json.NewDecoder(response.Body).Decode(&file); err != nil {
		return nil, err
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

// raw returns the client of requests which go-client can't make, such as
// an upload's, which has its transport, of its token, quota, and tracing,
// without its timeout, and which doesn't follow redirects, as Google's
// Resume Incomplete is a 308
func (c *DriveClient) raw() *http.Client {
	return &http.Client{
		Transport: c.Client.Client.Client.Transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// statusError returns nil of a response which succeeded, or the error of
// its status, as go-client's, with what its body says
func statusError(response *http.Response) error {
	if response.StatusCode >= 200 && response.StatusCode <= 299 {
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if text := strings.TrimSpace(string(data)); text != "" {
		return httpresponse.Err(response.StatusCode).With(text)
	}
	return httpresponse.Err(response.StatusCode).With(response.Status)
}

// parseVersion returns a file's version, as a number, or -1 when it has
// none, or it isn't one
func parseVersion(version string) int64 {
	if v, err := strconv.ParseInt(version, 10, 64); err == nil {
		return v
	}
	return -1
}

// NotOlder returns true when a file's version is the same as another's, or after it, or it can't
// be said, as either has no version
func (f *DriveFile) NotOlder(than *DriveFile) bool {
	a, b := parseVersion(f.Version), parseVersion(than.Version)
	return a < 0 || b < 0 || a >= b
}
