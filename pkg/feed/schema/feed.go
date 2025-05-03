package schema

import (
	"context"

	// Packages
	pg "github.com/djthorpe/go-pg"
)

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
			"id"        BIGSERIAL NOT NULL REFERENCES ${"schema"}."feed" ("id") ON DELETE CASCADE,
			"ts"        TIMESTAMP, 			-- Last fetched timestamp
			"period"    INTERVAL NOT NULL,  -- Fetch period
			"hash" 	    TEXT, 			    -- Hash of the feed
			PRIMARY KEY ("id") 			    -- Ensure ID constraint
		)
	`
)
