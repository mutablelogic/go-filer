package httphandler

import (
	"bytes"
	_ "embed"
	"html/template"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// listPage is a page of the objects in a directory of a volume, as HTML
type listPage struct {
	Name     string
	Path     string
	Readonly bool       // when objects can't be uploaded or deleted
	Upload   string     // the link of the directory, which a file is uploaded into
	Crumb    []listLink // the directories of the path, from the root
	Rows     []listRow
	Next     string // the URL of the next page, when there is one
}

// listLink is a link to a directory
type listLink struct {
	Name, Href string
}

// listRow is an object in the page, whose name is its key, relative to the
// directory which is listed, and which links to its listing when it's a
// directory, or its content when it's a file
type listRow struct {
	Name, Href, Type, Size, Modified string
	Download                         string // the link which downloads a file
	Delete                           string // the link which deletes the object, recursively for a directory
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed list.html
var listHTML string

var listTemplate = template.Must(template.New("list").Parse(listHTML))

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// acceptsHTML returns true when the request accepts text/html before JSON,
// as a browser does, by the order of their media ranges, which aren't
// accepted when their quality is 0, and otherwise ignoring it
func acceptsHTML(r *http.Request) bool {
	for _, value := range strings.Split(r.Header.Get("Accept"), ",") {
		if mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(value)); err != nil {
			continue
		} else if q, err := strconv.ParseFloat(params["q"], 64); err == nil && q <= 0 {
			continue
		} else if mediaType == "text/html" || mediaType == "application/xhtml+xml" {
			return true
		} else if mediaType == "application/json" || mediaType == "*/*" {
			return false
		}
	}
	return false
}

// writeListHTML responds with the page of objects as HTML. Links are
// relative to the request's URL, keeping its query, but the path and where
// the page starts. Unless the volume is read-only, each object can be
// deleted, which a script asks to confirm, and a directory is deleted with
// every object beneath it.
func writeListHTML(w http.ResponseWriter, r *http.Request, list *schema.ObjectList, readonly bool) error {
	query := r.URL.Query()
	query.Del("start_after")
	href := func(dir string) string {
		q := cloneValues(query)
		if dir == "/" {
			q.Del("path")
		} else {
			q.Set("path", dir)
		}
		if encoded := q.Encode(); encoded != "" {
			return "?" + encoded
		}
		return "?"
	}

	// The directories of the path, from the root
	page := listPage{Name: list.Name, Path: list.Path, Readonly: readonly}
	// Files are uploaded into the directory which is listed, but not when
	// the listing is of a file
	if !readonly && !(len(list.Body) == 1 && !list.Body[0].IsDir && list.Body[0].Path == list.Path) {
		page.Upload = dirHref(list.Name, list.Path)
	}
	// The directories of the path link to their listings, with "/", as a
	// bucket can have an object of a directory's name, and the last element
	// links to the path which is listed, which is a file, or a directory with
	// or without "/", as it was requested
	page.Crumb = append(page.Crumb, listLink{Name: list.Name, Href: href("/")})
	dir := "/"
	elems := strings.Split(strings.Trim(list.Path, "/"), "/")
	for i, elem := range elems {
		if elem == "" {
			continue
		}
		dir = path.Join(dir, elem)
		if i == len(elems)-1 {
			page.Crumb = append(page.Crumb, listLink{Name: elem, Href: href(list.Path)})
		} else {
			page.Crumb = append(page.Crumb, listLink{Name: elem, Href: href(dir + "/")})
		}
	}

	// The objects, whose names are relative to the directory
	prefix := strings.TrimSuffix(list.Path, "/") + "/"
	for _, object := range list.Body {
		row := listRow{
			Name:     strings.TrimPrefix(object.Key(), prefix),
			Type:     cellText(object, 1),
			Size:     cellText(object, 2),
			Modified: cellText(object, 3),
		}
		if row.Name == "" {
			row.Name = object.Key()
		}
		if object.IsDir {
			row.Href = href(object.Key())
			row.Delete = dirHref(list.Name, object.Path) + "?recursive=true"
		} else {
			row.Href = fileHref(list.Name, object.Path)
			row.Download = row.Href + "?download=true"
			row.Delete = row.Href
		}
		if readonly {
			row.Delete = ""
		}
		page.Rows = append(page.Rows, row)
	}

	// The next page
	if list.Next != "" {
		q := cloneValues(r.URL.Query())
		q.Set("start_after", list.Next)
		page.Next = "?" + q.Encode()
	}

	// Write the page, which is rendered first, so an error is a response
	var buf bytes.Buffer
	if err := listTemplate.Execute(&buf, page); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(buf.Bytes())
	return err
}

// fileHref returns the link to the content of a file, which is relative to
// the listing, object/<name>, so it's object/<name>/<path>, with each element
// escaped
func fileHref(name, path string) string {
	elems := []string{url.PathEscape(name)}
	for _, elem := range strings.Split(strings.Trim(path, "/"), "/") {
		elems = append(elems, url.PathEscape(elem))
	}
	return strings.Join(elems, "/")
}

// dirHref returns the link of a directory, which files are uploaded into,
// which is relative to the listing, object/<name>, so it's
// object/<name>/<path>/, with each element escaped, or object/<name>/ for
// the root
func dirHref(name, path string) string {
	if strings.Trim(path, "/") == "" {
		return url.PathEscape(name) + "/"
	}
	return fileHref(name, path) + "/"
}

// cellText returns the text of a column of an object's row in a table
func cellText(object *schema.Object, col int) string {
	if text, ok := object.Cell(col).(string); ok {
		return text
	}
	return ""
}

// cloneValues returns a copy of the query
func cloneValues(values url.Values) url.Values {
	result := make(url.Values, len(values))
	for key, value := range values {
		result[key] = append([]string(nil), value...)
	}
	return result
}
