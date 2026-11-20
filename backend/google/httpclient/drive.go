package httpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// DriveClient is a client of the Google Drive API, of a person's files
type DriveClient struct {
	*authclient.Client
}

// DriveFile is a file, or a folder, of the fields which are asked for
// Reference: https://developers.google.com/drive/api/reference/rest/v3/files
type DriveFile struct {
	Id           string       `json:"id"`
	Name         string       `json:"name"`
	MimeType     string       `json:"mimeType"`
	Parents      []string     `json:"parents,omitempty"`
	Size         string       `json:"size,omitempty"` // Of a file which isn't Google's own, such as a Doc, in bytes, as a string
	MD5Checksum  string       `json:"md5Checksum,omitempty"`
	ModifiedTime time.Time    `json:"modifiedTime,omitzero"`
	Version      string       `json:"version,omitempty"`        // Which increases whenever it's changed
	HeadRevision string       `json:"headRevisionId,omitempty"` // Of its content, of a file which isn't Google's own
	WebViewLink  string       `json:"webViewLink,omitempty"`
	Owners       []DriveOwner `json:"owners,omitempty"`
	Trashed      bool         `json:"trashed,omitempty"`   // Of a change, as the files listed aren't
	OwnedByMe    bool         `json:"ownedByMe,omitempty"` // Of a change, as the files listed are
}

// DriveChange is a change to a file: it's removed, such as when it's
// deleted, or no longer shared, or it's the file as it is now, which can be
// in the bin, or another's
type DriveChange struct {
	FileId  string    `json:"fileId"`
	Removed bool      `json:"removed,omitempty"`
	File    DriveFile `json:"file"`
}

// DriveOwner is who owns a file
type DriveOwner struct {
	EmailAddress string `json:"emailAddress,omitempty"`
}

// driveChanges is a page of changes, the last of which has the token of
// the changes after them
type driveChanges struct {
	Changes           []DriveChange `json:"changes,omitempty"`
	NextPageToken     string        `json:"nextPageToken,omitempty"`
	NewStartPageToken string        `json:"newStartPageToken,omitempty"`
}

// driveFiles is a page of files
type driveFiles struct {
	Files         []DriveFile `json:"files,omitempty"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// errNoToken is the error of a response without the token of the changes
// after it, which it must have
var errNoToken = errors.New("no token of the changes was returned")

// bodyReader reads a response's body, as it's received, of a function
type bodyReader func(io.Reader) error

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// DriveEndpoint is the endpoint of the Google Drive API
	DriveEndpoint = "https://www.googleapis.com/drive/v3"

	// DriveScope is the scope of reading and writing files, and
	// DriveReadonlyScope of reading them
	DriveScope         = "https://www.googleapis.com/auth/drive"
	DriveReadonlyScope = "https://www.googleapis.com/auth/drive.readonly"

	// DriveFolderType is the type of a folder, and DriveAppsPrefix the
	// prefix of the types of Google's own files, such as Docs, and
	// shortcuts, which have no content of their own
	DriveFolderType = "application/vnd.google-apps.folder"
	DriveAppsPrefix = "application/vnd.google-apps."

	// driveFileFields are the fields of each file which are got, and
	// driveFields of a page of them, and driveChangeFields of a page of
	// changes
	driveFileFields   = "id,name,mimeType,parents,size,md5Checksum,modifiedTime,version,headRevisionId,webViewLink,owners(emailAddress),trashed,ownedByMe"
	driveFields       = "nextPageToken,files(" + driveFileFields + ")"
	driveChangeFields = "nextPageToken,newStartPageToken,changes(fileId,removed,file(" + driveFileFields + "))"

	// drivePageSize is the most files of each page, which is the API's most
	drivePageSize = 1000
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewDriveClient returns a client of the Google Drive API, at its endpoint,
// or DriveEndpoint, when it's empty, with go-client's options, such as a
// token, a tracer, or a rate limit
func NewDriveClient(endpoint string, opts ...client.ClientOpt) (*DriveClient, error) {
	if endpoint = strings.TrimSpace(endpoint); endpoint == "" {
		endpoint = DriveEndpoint
	}
	c, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &DriveClient{Client: c}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Root returns the id of the folder of My Drive, which is how the files
// are checked to be readable, or an AuthError, when a sign-in is needed
func (c *DriveClient) Root(ctx context.Context) (string, error) {
	var root DriveFile
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &root,
		client.OptPath("files", "root"),
		client.OptQuery(url.Values{"fields": {"id"}}),
	); err != nil {
		return "", err
	}
	return root.Id, nil
}

// Files returns the files, and folders, which the person owns, which
// aren't in the bin, a page at a time. Files which others own, such as
// those shared with the person, aren't returned.
func (c *DriveClient) Files(ctx context.Context) ([]DriveFile, error) {
	var result []DriveFile
	query := url.Values{
		"q":        {"trashed = false and 'me' in owners"},
		"fields":   {driveFields},
		"pageSize": {strconv.Itoa(drivePageSize)},
		"spaces":   {"drive"},
	}
	for range maxPages {
		var page driveFiles
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("files"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		result = append(result, page.Files...)
		if page.NextPageToken == "" {
			return result, nil
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return nil, errTooManyPages
}

// StartPageToken returns the token of the changes from now
func (c *DriveClient) StartPageToken(ctx context.Context) (string, error) {
	var result struct {
		StartPageToken string `json:"startPageToken"`
	}
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result,
		client.OptPath("changes", "startPageToken"),
	); err != nil {
		return "", err
	} else if result.StartPageToken == "" {
		return "", errNoToken
	}
	return result.StartPageToken, nil
}

// Changes returns the changes to the files of My Drive since the token, a
// page at a time, and the token of the changes after them. A token which
// Google doesn't accept, such as one which has expired, is 400 or 404.
func (c *DriveClient) Changes(ctx context.Context, token string) ([]DriveChange, string, error) {
	var result []DriveChange
	query := url.Values{
		"pageToken":         {token},
		"fields":            {driveChangeFields},
		"pageSize":          {strconv.Itoa(drivePageSize)},
		"spaces":            {"drive"},
		"restrictToMyDrive": {"true"},
		"includeRemoved":    {"true"},
	}
	for range maxPages {
		var page driveChanges
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("changes"),
			client.OptQuery(query),
		); err != nil {
			return nil, "", err
		}
		result = append(result, page.Changes...)
		if page.NextPageToken != "" {
			query.Set("pageToken", page.NextPageToken)
			continue
		} else if page.NewStartPageToken == "" {
			return nil, "", errNoToken
		}
		return result, page.NewStartPageToken, nil
	}
	return nil, "", errTooManyPages
}

// Version returns the version of a file, of its id, which increases
// whenever it's changed
func (c *DriveClient) Version(ctx context.Context, id string) (string, error) {
	var file DriveFile
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &file,
		client.OptPath("files", id),
		client.OptQuery(url.Values{"fields": {"version"}}),
	); err != nil {
		return "", err
	}
	return file.Version, nil
}

// Download reads the content of a file, of its id, which isn't Google's
// own, such as a Doc, of a revision, or the current one, when it's empty,
// with the function, as it's received, without a timeout, other than the
// context's, as a file can be large
func (c *DriveClient) Download(ctx context.Context, id, revision string, fn func(io.Reader) error) error {
	path := []any{"files", id}
	if revision != "" {
		path = append(path, "revisions", revision)
	}
	return c.DoAuthWithContext(ctx, client.NewRequest(), bodyReader(fn),
		client.OptPath(path...),
		client.OptQuery(url.Values{"alt": {"media"}}),
		client.OptNoTimeout(),
	)
}

// Export reads the content of a Google Doc, Sheet or Slides, of its id, as
// the type, such as a Word document, with the function, as it's received.
// Google exports at most 10 MB, and returns 403 for more.
func (c *DriveClient) Export(ctx context.Context, id, mimeType string, fn func(io.Reader) error) error {
	return c.DoAuthWithContext(ctx, client.NewRequest(), bodyReader(fn),
		client.OptPath("files", id, "export"),
		client.OptQuery(url.Values{"mimeType": {mimeType}}),
		client.OptNoTimeout(),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// Unmarshal reads a response's body, whatever its type, as go-client
// prefers it to decoding it
func (fn bodyReader) Unmarshal(_ http.Header, r io.Reader) error {
	return fn(r)
}
