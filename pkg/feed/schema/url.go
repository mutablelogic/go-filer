package schema

import (
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type UrlMeta struct {
	Url *string `json:"url" arg:"" help:"Feed URL to be added"`
}

type UrlId uint64

type Url struct {
	Id uint64 `json:"id"`
	UrlMeta
	Ts time.Time `json:"ts,omitzero"`
}

type UrlListRequest struct {
	pg.OffsetLimit
}

type UrlList struct {
	Count uint64 `json:"count"`
	Body  []Url  `json:"body,omitempty" help:"List of URLs"`
}

//////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (u Url) String() string {
	data, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (u UrlMeta) String() string {
	data, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (u UrlListRequest) String() string {
	data, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (u UrlList) String() string {
	data, err := json.MarshalIndent(u, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

//////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (u UrlId) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if u == 0 {
		return "", httpresponse.ErrBadRequest.Withf("invalid id: %v", u)
	} else {
		bind.Set("id", u)
	}

	switch op {
	case pg.Get:
		return urlGet, nil
	case pg.Delete:
		return urlDelete, nil
	default:
		return "", httpresponse.ErrNotImplemented.Withf("UrlId operation: %q", op)
	}
}

func (u UrlListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Where and Orderby
	bind.Set("where", "")
	bind.Set("orderby", "ORDER BY ts DESC")

	// Bind offset and limit
	u.OffsetLimit.Bind(bind, ItemListLimit)

	switch op {
	case pg.List:
		return urlList, nil
	default:
		return "", httpresponse.ErrNotImplemented.Withf("UrlListRequest operation: %q", op)
	}
}

//////////////////////////////////////////////////////////////////////////////////
// READER

func (u *Url) Scan(row pg.Row) error {
	return row.Scan(&u.Id, &u.Ts, &u.Url)
}

func (u *UrlList) Scan(row pg.Row) error {
	var url Url
	if err := url.Scan(row); err != nil {
		return err
	}
	u.Body = append(u.Body, url)
	return nil
}

func (n *UrlList) ScanCount(row pg.Row) error {
	return row.Scan(&n.Count)
}

//////////////////////////////////////////////////////////////////////////////////
// WRITER

// Insert
func (u UrlMeta) Insert(bind *pg.Bind) (string, error) {
	// Queue name
	if url, err := url.Parse(*u.Url); err != nil || !slices.Contains(acceptableSchemes, url.Scheme) {
		return "", httpresponse.ErrBadRequest.Withf("invalid url: %q", url)
	} else {
		bind.Set("url", url.String())
	}

	// Return the insert query
	return urlInsert, nil
}

func (u UrlMeta) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented
}

//////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapUrl(ctx context.Context, conn pg.Conn) error {
	q := []string{
		urlCreateTable,
	}
	for _, query := range q {
		if err := conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

const (
	urlCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."url" (
			"id"       BIGSERIAL PRIMARY KEY,    			         -- Identifier
			"ts"       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Added/updated timestamp                             
			"url"      TEXT NOT NULL,                                -- URL
			UNIQUE("url")                                            -- Ensure the URL is unique                            
                                                                              
		)
	`
	urlInsert = `
		INSERT INTO ${"schema"}."url" 
			("ts", "url") 
		VALUES 
			(DEFAULT, @url)	
		RETURNING
			"id", "ts", "url"
	`
	urlSelect = `SELECT "id", "ts", "url" FROM ${"schema"}."url"`
	urlGet    = urlSelect + ` WHERE id = @id`
	urlList   = `WITH q AS (` + urlSelect + `) SELECT * FROM q ${where} ${orderby}`
	urlDelete = `DELETE FROM ${"schema"}."url" WHERE id = @id RETURNING "id", "ts", "url"`
)
