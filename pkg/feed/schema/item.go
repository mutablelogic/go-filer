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
)

//////////////////////////////////////////////////////////////////////////////////
// TYPES

type RSSItem rss.Item

type ItemMeta struct {
	Title    string         `json:"title" help:"Title"`
	PubDate  time.Time      `json:"pubdate" help:"Publication date"`
	Author   *string        `json:"author,omitempty" help:"Author"`
	Link     *string        `json:"linkomitempty" help:"Link"`
	Desc     *string        `json:"descomitempty" help:"Description"`
	Type     *string        `json:"typeomitempty" help:"Type"`
	Duration *time.Duration `json:"duration,omitzero" help:"Duration"`
	Block    bool           `json:"block" help:"Block - item should not be shown"`
	Meta     map[string]any `json:"meta" help:"Meta"`
}

type ItemId struct {
	Guid string `json:"guid"`
	Feed uint64 `json:"feed"`
}

type Item struct {
	ItemId
	ItemMeta
	Hash *string   `json:"hash,omitempty"`
	Ts   time.Time `json:"ts,omitzero"`
}

type ItemList struct {
	Count int64  `json:"count" help:"Count of items"`
	Body  []Item `json:"body,omitempty" name:"body" help:"List of items"`
}

//////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (i Item) String() string {
	data, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (i ItemMeta) String() string {
	data, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (i ItemId) String() string {
	data, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (i ItemList) String() string {
	data, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

//////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (i ItemId) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if i.Guid == "" {
		return "", httpresponse.ErrBadRequest.With("Missing guid")
	} else {
		bind.Set("guid", i.Guid)
	}
	if i.Feed == 0 {
		return "", httpresponse.ErrBadRequest.With("Missing feed")
	} else {
		bind.Set("feed", i.Feed)
	}

	switch op {
	case pg.Get:
		return itemGet, nil
	default:
		return "", httpresponse.ErrBadRequest.Withf("ItemId: Invalid operation %q", op)
	}
}

//////////////////////////////////////////////////////////////////////////////////
// READER

func (i *Item) Scan(row pg.Row) error {
	return row.Scan(&i.Guid, &i.Feed, &i.Hash, &i.Ts, &i.Title, &i.PubDate, &i.Author, &i.Link, &i.Desc, &i.Type, &i.Duration, &i.Block, &i.Meta)
}

//////////////////////////////////////////////////////////////////////////////////
// WRITER

func (i RSSItem) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("feed") {
		return "", httpresponse.ErrBadRequest.With("Missing feed")
	}

	// Required fields
	if guid := strings.TrimSpace(i.GUID.Value); guid == "" {
		return "", httpresponse.ErrBadRequest.With("Missing guid")
	} else {
		bind.Set("guid", guid)
	}
	if title := strings.TrimSpace(i.Title); title == "" {
		bind.Set("title", bind.Get("guid"))
	} else {
		bind.Set("title", title)
	}
	if i.PubDate == nil {
		bind.Set("pubdate", time.Now().UTC())
	} else if date, err := i.PubDate.Parse(); err != nil {
		return "", httpresponse.ErrBadRequest.With("invalid pubdate")
	} else {
		bind.Set("pubdate", date.UTC())
	}

	// Optional fields
	if author := strings.TrimSpace(i.Author); author != "" {
		bind.Set("author", author)
	} else {
		bind.Set("author", nil)
	}
	if len(i.Link) > 0 {
		if i.Link[0].Value != "" {
			bind.Set("link", i.Link[0].Value)
		}
	} else {
		bind.Set("link", nil)
	}
	if desc := strings.TrimSpace(i.Description); desc != "" {
		bind.Set("desc", desc)
	} else {
		bind.Set("desc", nil)
	}
	if typ := strings.TrimSpace(i.EpisodeType); typ != "" {
		bind.Set("type", typ)
	} else {
		bind.Set("type", nil)
	}
	if dur := i.Duration; dur != nil {
		if secs, err := dur.Seconds(); err != nil {
			return "", httpresponse.ErrBadRequest.With("invalid duration")
		} else {
			bind.Set("duration", secs)
		}
	} else {
		bind.Set("duration", nil)
	}

	// Item is blocked from download
	bind.Set("block", rss.ParseBool(i.Block))

	// Set JSON metadata
	data, err := json.Marshal(i)
	if err != nil {
		return "", httpresponse.ErrBadRequest.Withf("error marshalling item: %v", err)
	} else {
		bind.Set("meta", json.RawMessage(string(data)))
	}

	// Return success
	return itemUpsert, nil
}

func (i RSSItem) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("RSSItem.Update")
}

//////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapItem(ctx context.Context, conn pg.Conn) error {
	q := []string{
		itemCreateTable,
		itemUpsertTriggerFunction,
		itemUpsertTrigger,
	}
	for _, query := range q {
		if err := conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

const (
	itemCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."item" (
			-- Primary key
			"guid"      TEXT NOT NULL, 
			"feed"      BIGSERIAL NOT NULL REFERENCES ${"schema"}."feed" ("id") ON DELETE CASCADE,  
			-- JSON hash and timestamp
			"hash"	    TEXT,
			"ts"        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			-- Item fields
			"title"     TEXT NOT NULL,
			"pubdate"   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			"author"    TEXT,
			"link"      TEXT,
			"desc"      TEXT,
			"type"      TEXT,
			"duration"  INTERVAL,
			"block"     BOOLEAN NOT NULL DEFAULT FALSE,
			"meta"      JSONB NOT NULL DEFAULT '{}'::JSONB,
			-- Primary key
			PRIMARY KEY ("guid", "feed")
		)
	`
	itemUpsert = `
		INSERT INTO ${"schema"}."item" 
			("guid", "feed",  "hash", "ts", "title", "pubdate", "author", "link", "desc", "type", "duration", "block", "meta")
		VALUES
			(@guid,  @feed,  MD5(@meta::TEXT), DEFAULT, @title, @pubdate, @author, @link, @desc, @type, @duration, @block, @meta::JSONB)
		ON CONFLICT ("guid", "feed") DO UPDATE SET
			"hash" = MD5(@meta::TEXT),
			"ts" = CURRENT_TIMESTAMP,
			"title" = @title,
			"pubdate" = @pubdate,
			"author" = @author,
			"link" = @link,
			"desc" = @desc,
			"type" = @type,
			"duration" = @duration,
			"block" = @block,
			"meta" = @meta::JSONB
		WHERE
			${"schema"}."item"."hash" IS DISTINCT FROM MD5(@meta::TEXT) -- This condition ensures update only happens if hash differs
		RETURNING
			"guid", "feed",  "hash", "ts", "title", "pubdate", "author", "link", "desc", "type", "duration", "block", "meta"
	`
	itemUpsertTriggerFunction = `
		CREATE OR REPLACE FUNCTION ${"schema"}.item_upsert() RETURNS TRIGGER AS $$
		DECLARE
        	payload JSONB;		
		BEGIN
			-- Create a JSON object containing only guid and feed
            payload := jsonb_build_object(
                'guid', NEW.guid,
                'feed', NEW.feed
            );
			-- Insert the payload into the queue
			PERFORM ${"pgqueue_schema"}.queue_insert(${'ns'}, ${'taskname_fetch_item'}, payload, NULL);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`
	itemUpsertTrigger = `
		CREATE OR REPLACE TRIGGER 
			item_upsert AFTER INSERT OR UPDATE ON ${"schema"}."item"
		FOR EACH ROW EXECUTE PROCEDURE
			${"schema"}.item_upsert() 
	`
	itemSelect = `
		SELECT
			"guid", "feed",  "hash", "ts", "title", "pubdate", "author", "link", "desc", "type", "duration", "block", "meta"		
		FROM
			${"schema"}."item"
    `
	itemGet  = feedSelect + `WHERE "guid" = @guid AND "feed" = @feed`
	itemList = feedSelect + `${where} ${orderby}`
)
