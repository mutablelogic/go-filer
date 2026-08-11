package google

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// driveFile is the subset of the Google Drive API "files" resource we need.
type driveFile struct {
	Id           string `json:"id"`
	Name         string `json:"name"`
	MimeType     string `json:"mimeType"`
	Size         string `json:"size,omitempty"`         // int64 as a string, absent for folders
	ModifiedTime string `json:"modifiedTime,omitempty"` // RFC3339
	Md5Checksum  string `json:"md5Checksum,omitempty"`
}

type driveFileList struct {
	NextPageToken string      `json:"nextPageToken,omitempty"`
	Files         []driveFile `json:"files,omitempty"`
}

// driveQueueEntry is a folder still to be visited when listing recursively.
type driveQueueEntry struct {
	FolderId string
	Path     string // path of this folder, relative to the listing root
}

// driveListToken is the pagination/traversal state stashed on the iterator between calls.
type driveListToken struct {
	Queue     []driveQueueEntry
	PageToken string // page token for the folder at the front of Queue
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const driveFolderMimeType = "application/vnd.google-apps.folder"

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Iterate through the list of objects in the backend, until io.EOF is returned.
func (c *Client) ListObjects(ctx context.Context, iterator *schema.ObjectListIterator) (err error) {
	ctx, endSpan := otel.StartSpan(c.tracer, ctx, "google.ListObjects",
		attribute.String("req", types.Stringify(iterator)),
	)
	defer func() {
		if errors.Is(err, io.EOF) {
			endSpan(nil)
		} else {
			endSpan(err)
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}

	tok, ok := iterator.Token.(*driveListToken)
	if tok == nil || !ok {
		basePrefix := strings.Trim(c.url.Path, "/")
		reqPath := strings.Trim(strings.TrimSpace(types.Value(iterator.Path)), "/")

		fullPath := basePrefix
		if reqPath != "" {
			if fullPath != "" {
				fullPath += "/" + reqPath
			} else {
				fullPath = reqPath
			}
		}

		rootId, err := c.resolveFolder(ctx, fullPath)
		if err != nil {
			return err
		}

		tok = &driveListToken{Queue: []driveQueueEntry{{FolderId: rootId}}}
		iterator.Token = tok
	}

	iterator.Body = make([]*schema.Object, 0, schema.ObjectListLimit)
	listingDirs := types.Value(iterator.Type) == schema.ContentTypeDirectory

	if len(tok.Queue) == 0 {
		iterator.Token = nil
		return io.EOF
	}

	// Fetch a single page of a single folder per call, rather than draining the
	// whole traversal queue here. A folder tree can be arbitrarily deep and wide,
	// each folder costing one Drive API round trip; doing that all within one
	// call could block for a very long time. Returning after one page lets the
	// caller's "call until io.EOF" loop pace the traversal instead.
	entry := tok.Queue[0]

	query := url.Values{}
	query.Set("q", fmt.Sprintf("%s and trashed = false", driveParentQuery(entry.FolderId)))
	query.Set("fields", "nextPageToken, files(id,name,mimeType,size,modifiedTime,md5Checksum)")
	query.Set("pageSize", strconv.Itoa(schema.ObjectListLimit))
	query.Set("spaces", "drive")
	if tok.PageToken != "" {
		query.Set("pageToken", tok.PageToken)
	}

	authOpt, err := c.authOpt()
	if err != nil {
		return err
	}

	var out driveFileList
	if err := c.DoAuthWithContext(ctx, client.MethodGet, &out,
		client.OptAbsPath("drive", "v3", "files"),
		client.OptQuery(query),
		authOpt,
	); err != nil {
		return err
	}

	for _, file := range out.Files {
		isDir := file.MimeType == driveFolderMimeType
		childPath := path.Join(entry.Path, file.Name)

		if isDir && iterator.Recursive {
			tok.Queue = append(tok.Queue, driveQueueEntry{FolderId: file.Id, Path: childPath})
		}
		if isDir != listingDirs {
			continue
		}

		obj := &schema.Object{
			ObjectKey: schema.ObjectKey{Volume: c.Name(), Path: childPath},
		}
		if isDir {
			obj.ObjectMeta.ContentType = schema.ContentTypeDirectory
		} else {
			obj.ObjectMeta.ContentType = file.MimeType
			if size, err := strconv.ParseInt(file.Size, 10, 64); err == nil {
				obj.ObjectAttr.Size = types.Ptr(size)
			}
			if file.Md5Checksum != "" {
				obj.ObjectAttr.ETag = types.Ptr(file.Md5Checksum)
			}
			if modTime, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
				obj.ModTime = modTime
			}
		}
		iterator.Body = append(iterator.Body, obj)
	}

	if out.NextPageToken != "" {
		tok.PageToken = out.NextPageToken
	} else {
		tok.Queue = tok.Queue[1:]
		tok.PageToken = ""
	}

	if len(tok.Queue) == 0 {
		iterator.Token = nil
		return io.EOF
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// resolveFolder walks a slash-separated path from the drive root, one name-lookup
// per segment, and returns the id of the folder at that path.
func (c *Client) resolveFolder(ctx context.Context, fullPath string) (string, error) {
	fullPath = strings.Trim(fullPath, "/")
	if fullPath == "" {
		return "root", nil
	}

	parentId := "root"
	for segment := range strings.SplitSeq(fullPath, "/") {
		if segment == "" {
			continue
		}

		query := url.Values{}
		query.Set("q", fmt.Sprintf("mimeType = %s and trashed = false and %s and name = %s",
			driveQuoteQueryString(driveFolderMimeType), driveParentQuery(parentId), driveQuoteQueryString(segment)))
		query.Set("fields", "files(id)")
		query.Set("pageSize", "1")
		query.Set("spaces", "drive")

		authOpt, err := c.authOpt()
		if err != nil {
			return "", err
		}

		var out driveFileList
		if err := c.DoAuthWithContext(ctx, client.MethodGet, &out,
			client.OptAbsPath("drive", "v3", "files"),
			client.OptQuery(query),
			authOpt,
		); err != nil {
			return "", err
		}
		if len(out.Files) == 0 {
			return "", gofiler.ErrNotFound.Withf("path not found: %q", fullPath)
		}

		parentId = out.Files[0].Id
	}

	return parentId, nil
}

// authOpt returns the request option which sets the bearer token on outgoing requests,
// fetching a fresh token from the token source (refreshing it if necessary).
func (c *Client) authOpt() (client.RequestOpt, error) {
	tok, err := c.tokenSource.Token()
	if err != nil {
		return nil, err
	}
	return client.OptToken(client.Token{Scheme: client.Bearer, Value: tok.AccessToken}), nil
}

// driveParentQuery returns the "q" clause used to list the children of folderId.
// Items shared directly with a service account (as opposed to owned by it, or
// living in a Shared Drive) never have "root" as a parent from that account's
// point of view, so the drive root has to be listed via sharedWithMe instead.
func driveParentQuery(folderId string) string {
	if folderId == "root" {
		return "sharedWithMe = true"
	}
	return fmt.Sprintf("%s in parents", driveQuoteQueryString(folderId))
}

// driveQuoteQueryString escapes and quotes a string for use in a Drive API "q" query,
// per https://developers.google.com/drive/api/guides/ref-search-terms.
func driveQuoteQueryString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
