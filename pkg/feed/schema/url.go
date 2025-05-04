package schema

import (
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type UrlMeta struct {
	Period time.Duration `json:"period,omitempty" help:"Fetch period"`
}

type UrlId uint64

type Url struct {
	Id     uint64     `json:"id"`
	Ts     time.Time  `json:"ts,omitzero" help:"Url created/updated timestamp"`
	Update *time.Time `json:"update,omitzero" help:"Feed updated timestamp"`
	Url    *string    `json:"url" arg:"" help:"Feed URL"`
	UrlMeta
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
	case pg.Update:
		return urlUpdate, nil
	default:
		return "", httpresponse.ErrNotImplemented.Withf("UrlId operation: %q", op)
	}
}

func (u UrlListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Where
	bind.Set("where", "")

	// Order by the feeds which are most recently updated
	bind.Set("orderby", "ORDER BY update DESC NULLS FIRST")

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
	return row.Scan(&u.Id, &u.Ts, &u.Update, &u.Url, &u.Period)
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

func (u UrlMeta) Insert(bind *pg.Bind) (string, error) {
	if feedurl := bind.Get("url"); feedurl == nil {
		return "", httpresponse.ErrBadRequest.With("Missing url")
	} else if feedurl, err := url.Parse(feedurl.(string)); err != nil || !slices.Contains(acceptableSchemes, feedurl.Scheme) {
		return "", httpresponse.ErrBadRequest.Withf("invalid url: %q", feedurl)
	} else {
		bind.Set("url", feedurl.String())
	}

	// Return the insert query
	return urlInsert, nil
}

func (u UrlMeta) Update(bind *pg.Bind) error {
	var patch []string

	// Period
	if period := u.Period; period > 0 {
		patch = append(patch, `"period" = `+bind.Set("period", period))
	}

	// Patch - if nothing to patch, we flag to update the feed
	if len(patch) == 0 {
		bind.Set("patch", `"update" = NULL`)
	} else {
		bind.Set("patch", strings.Join(patch, ", "))
	}

	// Return success
	return nil
}

//////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapUrl(ctx context.Context, conn pg.Conn) error {
	q := []string{
		urlCreateTable,
		urlCreateTriggerFunction,
		urlCreateTrigger,
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
			"update"   TIMESTAMP, 			                         -- Last fetched timestamp
			"url"      TEXT NOT NULL,                                -- URL
			"period"   INTERVAL NOT NULL DEFAULT INTERVAL '1 hour',  -- Fetch period
			UNIQUE("url")                                            -- Ensure the URL is unique
		)
	`
	urlCreateTriggerFunction = `
		CREATE OR REPLACE FUNCTION ${"schema"}.url_create() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM ${"pgqueue_schema"}.queue_insert(${'ns'}, ${'taskname_create_url'}, row_to_json(NEW)::JSONB, NULL);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`
	urlCreateTrigger = `
		CREATE OR REPLACE TRIGGER 
			url_create AFTER INSERT ON ${"schema"}."url"
		FOR EACH ROW EXECUTE PROCEDURE
			${"schema"}.url_create() 
	`
	urlInsert = `
		INSERT INTO ${"schema"}."url" 
			("ts", "url", "period") 
		VALUES 
			(DEFAULT, @url, DEFAULT)	
		RETURNING
			"id", "ts", "update", "url", "period"
	`
	urlUpdate = `
		 UPDATE 
		 	${"schema"}."url"
		SET
			"ts" = CURRENT_TIMESTAMP, ${patch} 
		WHERE
			"id" = @id
		RETURNING
			"id", "ts", "update", "url", "period"
	`
	urlDelete = `
		DELETE FROM 
			${"schema"}."url" 
		WHERE 
			id = @id 
		RETURNING 
			"id", "ts", "update", "url", "period"
	`
	urlSelect = `
		SELECT 
			"id", "ts", "update", "url", "period" 
		FROM 
			${"schema"}."url"
	`
	urlGet  = urlSelect + ` WHERE id = @id`
	urlList = `WITH q AS (` + urlSelect + `) SELECT * FROM q ${where} ${orderby}`
)
