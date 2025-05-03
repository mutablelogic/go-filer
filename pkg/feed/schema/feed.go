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
	Id uint64    `json:"id,omitempty"`
	Ts time.Time `json:"ts,omitzero"`
	FeedMeta
}

type FeedHash struct {
	Id     uint64        `json:"id"`
	Ts     *time.Time    `json:"ts,omitzero"`
	Period time.Duration `json:"period"`
	Hash   *string       `json:"hash,omitempty"`
}

type FeedListRequest struct {
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

func (f FeedHash) String() string {
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
// READER

func (f *Feed) Scan(row pg.Row) error {
	return row.Scan(&f.Id, &f.Ts, &f.Title, &f.Author, &f.Link, &f.Lang, &f.Desc, &f.Type, &f.SkipDays, &f.SkipHours, &f.BuildDate, &f.PubDate, &f.TTL, &f.Block, &f.Complete, &f.Meta)
}

func (f *FeedHash) Scan(row pg.Row) error {
	return row.Scan(&f.Id, &f.Ts, &f.Period, &f.Hash)
}

//////////////////////////////////////////////////////////////////////////////////
// WRITER

func (f FeedHash) Insert(bind *pg.Bind) (string, error) {
	if f.Id == 0 {
		return "", httpresponse.ErrBadRequest.Withf("missing id")
	} else {
		bind.Set("id", f.Id)
	}
	if !bind.Has("meta") {
		return "", httpresponse.ErrBadRequest.With("missing meta")
	}
	if f.Period == 0 {
		return "", httpresponse.ErrBadRequest.With("missing period")
	} else {
		bind.Set("period", f.Period)
	}

	// Return success
	return feedHashInsert, nil
}

func (f FeedHash) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("FeedHash.Update not implemented")
}

func (r RSSFeed) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("id") {
		return "", httpresponse.ErrBadRequest.With("missing id")
	}

	// Title
	if title := strings.TrimSpace(r.Channel.Title); title == "" {
		return "", httpresponse.ErrBadRequest.Withf("missing title")
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
			return "", httpresponse.ErrBadRequest.Withf("error parsing TTL value")
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

	// Metadata
	bind.Set("meta", r.Channel)

	// Return success
	return feedInsert, nil
}

func (rss RSSFeed) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("Feed.Update not implemented")
}

//////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapFeed(ctx context.Context, conn pg.Conn) error {
	q := []string{
		feedCreateTable,
		feedHashCreateTable,
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
	feedHashCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."feed_hash" (
			-- Identifier
			"id"        BIGSERIAL NOT NULL REFERENCES ${"schema"}."url" ON DELETE CASCADE,
			"ts"        TIMESTAMP, 			-- Last fetched timestamp
			"period"    INTERVAL NOT NULL,  -- Fetch period
			"hash" 	    TEXT, 			    -- Hash of the feed
			PRIMARY KEY ("id") 			    -- Ensure ID constraint
		)
	`
	feedInsert = `
		INSERT INTO ${"schema"}."feed" (
			"id", "title", "author", "link", "lang", "desc", "type", "skip_days", "skip_hours", "builddate", "pubdate", "ttl", "block", "complete", "meta"
		) VALUES (
		 	@id, @title, @author, @link, @lang, @desc, @type, @skip_days, @skip_hours, @builddate, @pubdate, @ttl, @block, @complete, @meta
		) ON CONFLICT ("id") DO UPDATE SET
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
			"meta" = @meta,
			"ts" = CURRENT_TIMESTAMP
		RETURNING
			"id", "ts", "title", "author", "link", "lang", "desc", "type", "skip_days", "skip_hours", "builddate", "pubdate", "ttl", "block", "complete", "meta"
	`
	feedHashInsert = `
		INSERT INTO ${"schema"}."feed_hash" (
			"id", "period", "hash"
		) VALUES (
		 	@id, @period, MD5(@meta::TEXT)
		) ON CONFLICT ("id") DO UPDATE SET
		 	"period" = @period,
			"hash" = MD5(@meta::TEXT),
			"ts" = CURRENT_TIMESTAMP
		RETURNING
			"id", "ts", "period", "hash"
	`
)
