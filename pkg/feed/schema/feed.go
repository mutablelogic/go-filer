package schema

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	rss "github.com/mutablelogic/go-filer/pkg/rss"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type RSSFeed rss.Feed

type FeedId uint64

type FeedMeta struct {
	Title     *string        `json:"title,omitempty"`
	Author    *string        `json:"author,omitempty"`
	Link      *string        `json:"link,omitempty"`
	Lang      *string        `json:"lang,omitempty"`
	Desc      *string        `json:"desc,omitempty"`
	Type      *string        `json:"type,omitempty"`
	SkipDays  []string       `json:"skip_days,omitempty"`
	SkipHours []string       `json:"skip_hours,omitempty"`
	BuildDate *time.Time     `json:"builddate,omitzero"`
	PubDate   *time.Time     `json:"pubdate,omitzero"`
	TTL       *time.Duration `json:"ttl,omitempty"`
	Block     *bool          `json:"block,omitempty"`
	Complete  *bool          `json:"complete,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

type Feed struct {
	Id   uint64    `json:"id,omitempty"`
	Ts   time.Time `json:"ts,omitzero"`
	Hash *string   `json:"hash,omitempty"`
	FeedMeta
}

type FeedListRequest struct {
	Enabled bool `json:"enabled,omitempty" help:"List feeds which are not marked as complete"`
	Stale   bool `json:"stale,omitempty" help:"List stale feeds"`
	pg.OffsetLimit
}

type FeedList struct {
	Count int64  `json:"count" help:"Count of items"`
	Body  []Feed `json:"body,omitempty" name:"body" help:"List of feeds"`
}

//////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (f FeedMeta) String() string {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (f Feed) String() string {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (f FeedListRequest) String() string {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (f FeedList) String() string {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

//////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (u FeedId) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if u == 0 {
		return "", httpresponse.ErrBadRequest.Withf("invalid id: %v", u)
	} else {
		bind.Set("id", u)
	}

	switch op {
	case pg.Get:
		return feedGet, nil
	case pg.Update:
		return feedUpdate, nil
	default:
		return "", httpresponse.ErrNotImplemented.Withf("FeedId operation: %q", op)
	}
}

func (f FeedListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Orderby
	bind.Set("orderby", `ORDER BY "ts" DESC`)

	// Where
	var where []string
	if f.Enabled {
		where = append(where, `"complete" IS FALSE`)
	}
	if len(where) > 0 {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	} else {
		bind.Set("where", "")
	}

	// Bind offset and limit
	f.OffsetLimit.Bind(bind, ItemListLimit)

	switch op {
	case pg.List:
		if f.Stale {
			return feedListStale, nil
		} else {
			return feedList, nil
		}
	default:
		return "", httpresponse.ErrNotImplemented.Withf("FeedListRequest operation: %q", op)
	}
}

//////////////////////////////////////////////////////////////////////////////////
// READER

func (f *Feed) Scan(row pg.Row) error {
	return row.Scan(&f.Id, &f.Ts, &f.Hash, &f.Title, &f.Author, &f.Link, &f.Lang, &f.Desc, &f.Type, &f.SkipDays, &f.SkipHours, &f.BuildDate, &f.PubDate, &f.TTL, &f.Block, &f.Complete, &f.Meta)
}

func (f *FeedList) Scan(row pg.Row) error {
	var feed Feed
	if err := feed.Scan(row); err != nil {
		return err
	}
	f.Body = append(f.Body, feed)
	return nil
}

func (f *FeedList) ScanCount(row pg.Row) error {
	return row.Scan(&f.Count)
}

//////////////////////////////////////////////////////////////////////////////////
// WRITER

func (r RSSFeed) Insert(bind *pg.Bind) (string, error) {
	return feedInsert, r.bind(bind)
}

func (r RSSFeed) Update(bind *pg.Bind) error {
	return r.bind(bind)
}

func (r RSSFeed) bind(bind *pg.Bind) error {
	if !bind.Has("id") {
		return httpresponse.ErrBadRequest.With("missing id")
	}

	// Title
	if title := strings.TrimSpace(r.Channel.Title); title == "" {
		return httpresponse.ErrBadRequest.Withf("missing title")
	} else {
		bind.Set("title", title)
	}

	// Author
	if author := strings.TrimSpace(r.Channel.Author); author != "" {
		bind.Set("author", author)
	}

	// Link
	if link := strings.TrimSpace(r.Channel.Link); link != "" {
		bind.Set("link", link)
	}

	// Lang
	if lang := strings.TrimSpace(r.Channel.Language); lang != "" {
		bind.Set("lang", lang)
	}

	// Desc
	if desc := strings.TrimSpace(r.Channel.Description); desc != "" {
		bind.Set("desc", desc)
	}

	// SkipDays
	if r.Channel.SkipDays != nil {
		bind.Set("skip_days", r.Channel.SkipDays.Day)
	} else {
		bind.Set("skip_days", []string{})
	}

	// SkipHours
	if r.Channel.SkipHours != nil {
		bind.Set("skip_hours", r.Channel.SkipHours.Hour)
	} else {
		bind.Set("skip_hours", []string{})
	}

	// BuildDate
	if r.Channel.LastBuildDate != nil {
		if ts, err := r.Channel.LastBuildDate.Parse(); err == nil {
			bind.Set("builddate", types.TimePtr(ts.UTC()))
		}
	}

	// PubDate
	if r.Channel.PubDate != nil {
		if ts, err := r.Channel.PubDate.Parse(); err == nil {
			bind.Set("pubdate", types.TimePtr(ts.UTC()))
		}
	}

	// TTL
	if ttl := r.Channel.TTL; ttl != nil {
		if secs, err := ttl.Seconds(); err != nil {
			return httpresponse.ErrBadRequest.Withf("error parsing TTL value")
		} else if secs > 0 {
			bind.Set("ttl", secs)
		}
	}

	// Type
	if typ := strings.TrimSpace(r.Channel.Type); typ != "" {
		bind.Set("type", typ)
	}

	// Block & Complete
	bind.Set("block", rss.ParseBool(r.Channel.Block))
	bind.Set("complete", rss.ParseBool(r.Channel.Complete))

	// Hash - contains the hash of the whole feed
	if data, err := json.Marshal(r.Channel); err != nil {
		return httpresponse.ErrBadRequest.Withf("error marshalling channel: %v", err)
	} else {
		bind.Set("hash", json.RawMessage(string(data)))
	}

	// Metadata - but remove the items from the feed
	channel := *r.Channel
	channel.Items = nil
	if data, err := json.Marshal(channel); err != nil {
		return httpresponse.ErrBadRequest.Withf("error marshalling channel: %v", err)
	} else {
		bind.Set("meta", json.RawMessage(string(data)))
	}

	// Return success
	return nil
}

//////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapFeed(ctx context.Context, conn pg.Conn) error {
	q := []string{
		feedCreateTable,
	}
	for _, query := range q {
		if err := conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

const (
	feedCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."feed" (
			-- Identifier
			"id"            BIGSERIAL NOT NULL REFERENCES ${"schema"}."url" ON DELETE CASCADE,  
			"ts"            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Added/updated timestamp       
			"hash" 	        TEXT, 			                              -- Hash of the feed                      
			"title"         TEXT NOT NULL,                                -- Title
			"author"        TEXT,                                         -- Author
			"link"          TEXT,                                         -- Link
			"lang"          TEXT,                                         -- Language
			"desc"          TEXT,                                         -- Description
			"type" 	        TEXT,                                         -- Type
			"skip_days"     TEXT[] NOT NULL DEFAULT '{}'::TEXT[],         -- Days to skip
			"skip_hours"    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],         -- Hours to skip
			"builddate"     TIMESTAMP,                                    -- Last build date
			"pubdate"       TIMESTAMP,                                    -- Publication date
			"ttl"           INTERVAL,                                     -- Time to live 
			"block"         BOOLEAN NOT NULL DEFAULT FALSE,    			  -- Block fetching of item
			"complete"      BOOLEAN NOT NULL DEFAULT FALSE, 			  -- Feed is complete
			"meta"          JSONB NOT NULL DEFAULT '{}'::JSONB,           -- Other metadata
			PRIMARY KEY ("id")                                			  -- Ensure ID constraint
		)
	`
	feedInsert = `
		INSERT INTO ${"schema"}."feed" (
			"id", "hash", "title", "author", "link", "lang", "desc", "type", "skip_days", "skip_hours", "builddate", "pubdate", "ttl", "block", "complete", "meta"
		) VALUES (
		 	@id, MD5(@hash::TEXT), @title, @author, @link, @lang, @desc, @type, @skip_days, @skip_hours, @builddate, @pubdate, @ttl, @block, @complete, @meta::JSONB
		) ON CONFLICT ("id") DO UPDATE SET
			"ts" = CURRENT_TIMESTAMP,
			"hash" = MD5(@hash::TEXT),
		 	"title" = @title,
			"author" = @author,
			"link" = @link,
			"lang" = @lang,
			"desc" = @desc,
			"type" = @type,
			"skip_days" = @skip_days,
			"skip_hours" = @skip_hours,
			"builddate" = @builddate,
			"pubdate" = @pubdate,
			"ttl" = @ttl,
			"block" = @block,
			"complete" = @complete,
			"meta" = @meta::JSONB
		WHERE
			${"schema"}."feed"."hash" IS DISTINCT FROM MD5(@hash::TEXT) -- This condition ensures update only happens if hash differs			
		RETURNING
			"id", "ts", "hash", "title", "author", "link", "lang", "desc", "type", "skip_days", "skip_hours", "builddate", "pubdate", "ttl", "block", "complete", "meta"
	`
	feedUpdate = feedInsert
	feedSelect = `
		SELECT
			"id", "ts", "hash", "title", "author",  "link", "lang", "desc", "type", "skip_days", "skip_hours", "builddate", "pubdate", "ttl", "block", "complete", "meta"
		FROM
			${"schema"}."feed"
    `
	feedGet         = feedSelect + `WHERE "id" = @id`
	feedList        = feedSelect + `${where} ${orderby}`
	feedSelectStale = `
		WITH "inner" AS (` + feedSelect + ` ${where}) SELECT
			F.*
		FROM
			"inner" F
		JOIN
			${"schema"}."url" U ON F.id = U.id
		WHERE
			U."update" IS NULL OR U."update" + U."period" < CURRENT_TIMESTAMP
		ORDER BY
			U."update" NULLS FIRST
	`
	feedListStale = `WITH q AS (` + feedSelectStale + `) SELECT * FROM q ${where}`
)
