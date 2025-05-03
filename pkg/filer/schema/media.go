package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type MediaMeta struct {
	Type        string         `json:"type" help:"Type of the media"`
	Duration    *time.Duration `json:"duration,omitempty" help:"Duration of the media"`
	Title       *string        `json:"title,omitempty" help:"Title of the media"`
	Album       *string        `json:"album,omitempty" help:"Album of the media"`
	Artist      *string        `json:"artist,omitempty" help:"Artist of the media"`
	Composer    *string        `json:"composer,omitempty" help:"Composer of the media"`
	Genre       *string        `json:"genre,omitempty" help:"Genre of the media"`
	Track       *uint64        `json:"track,omitempty" help:"Track number of the media"`
	Disc        *uint64        `json:"disc,omitempty" help:"Disc number of the media"`
	Year        *uint64        `json:"year,omitempty" help:"Year of the media"`
	Description *string        `json:"description,omitempty" help:"Synopsis"`
	Meta        map[string]any `json:"meta" help:"Media metadata"`
	Images      []MediaImage   `json:"images,omitempty" help:"Artwork and thumbnails"`
}

type Media struct {
	Bucket string `json:"bucket,omitempty" name:"bucket" help:"Bucket in which the object is stored"`
	Key    string `json:"key,omitempty" name:"key" help:"Object key"`
	MediaMeta
}

type MediaImage struct {
	Type string         `json:"type" help:"Type of the media"`
	Hash string         `json:"hash" help:"Hash of the media data"`
	Data []byte         `json:"data" help:"Media data"`
	Pos  *time.Duration `json:"pos,omitempty" help:"Position in the media if thumbnail"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (m Media) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m MediaMeta) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m MediaImage) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (m MediaMeta) GetMeta(key ...string) (any, bool) {
	if m.Meta == nil {
		return nil, false
	}
	for _, k := range key {
		if value, ok := m.Meta[k]; ok {
			return value, true
		}
		lowerkey := strings.ToLower(k)
		for key, value := range m.Meta {
			if lowerkey == strings.ToLower(key) {
				return value, true
			}
		}
	}
	return nil, false
}

func (m MediaMeta) GetMetaString(key ...string) (string, bool) {
	if value, exists := m.GetMeta(key...); exists {
		switch value := value.(type) {
		case string:
			return value, true
		case []byte:
			return string(value), true
		default:
			return fmt.Sprint(value), true
		}
	}
	return "", false
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (m *Media) Scan(row pg.Row) error {
	return row.Scan(&m.Bucket, &m.Key, &m.Type, &m.Duration, &m.Title, &m.Album, &m.Artist, &m.Composer, &m.Genre, &m.Track, &m.Disc, &m.Year, &m.Description, &m.Meta)
}

func (m *MediaImage) Scan(row pg.Row) error {
	return row.Scan(&m.Hash, &m.Type, &m.Data, &m.Pos)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (m MediaMeta) Insert(bind *pg.Bind) (string, error) {
	// Check bind for bucket and key
	if !bind.Has("bucket") {
		return "", httpresponse.ErrBadRequest.With("missing bucket")
	} else if !bind.Has("key") {
		return "", httpresponse.ErrBadRequest.With("missing key")
	} else if m.Type == "" {
		return "", httpresponse.ErrBadRequest.With("missing type")
	}

	// Reset metadata
	if m.Meta == nil {
		m.Meta = make(map[string]any)
	}

	// Bind the metadata
	bind.Set("type", m.Type)
	bind.Set("duration", m.Duration)
	bind.Set("meta", m.Meta)
	bind.Set("title", m.Title)
	bind.Set("album", m.Album)
	bind.Set("artist", m.Artist)
	bind.Set("composer", m.Composer)
	bind.Set("genre", m.Genre)
	bind.Set("track", m.Track)
	bind.Set("disc", m.Disc)
	bind.Set("year", m.Year)
	bind.Set("description", m.Description)

	// Return success
	return mediaInsert, nil
}

func (m MediaImage) Insert(bind *pg.Bind) (string, error) {
	// Check parameters
	if m.Hash == "" {
		return "", httpresponse.ErrBadRequest.With("missing hash")
	} else if m.Type == "" {
		return "", httpresponse.ErrBadRequest.With("missing type")
	} else if m.Data == nil {
		return "", httpresponse.ErrBadRequest.With("missing data")
	}

	// Bind the metadata
	bind.Set("hash", m.Hash)
	bind.Set("type", m.Type)
	bind.Set("data", m.Data)
	bind.Set("pos", m.Pos)

	// Return success
	return mediaImageInsert, nil
}

func (m MediaMeta) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaMeta.Update not implemented")
}

func (m MediaImage) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaImage.Update not implemented")
}

////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapMedia(ctx context.Context, conn pg.Conn) error {
	q := []string{
		mediaCreateTable,
		mediaImageCreateTable,
	}
	for _, query := range q {
		if err := conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

const (
	mediaCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."media" (
			"bucket"        TEXT NOT NULL,
			"key"           TEXT NOT NULL,
			-- media metadata
			"type"          TEXT NOT NULL,
			"duration"      INTERVAL,
			"title"		 	TEXT,
			"album"         TEXT,
			"artist"        TEXT, -- also director
			"composer"	  	TEXT,
			"genre"         TEXT,
			"track"		    INTEGER, -- also episode
			"disc"		    INTEGER, -- also season
			"year"		    INTEGER,
			"description"   TEXT,
			"meta"          JSONB DEFAULT '{}'::JSONB NOT NULL,
			-- primary key
			PRIMARY KEY ("bucket", "key"),
			-- foreign key
			FOREIGN KEY ("bucket", "key") REFERENCES ${"schema"}."object" ("bucket", "key") ON DELETE CASCADE
		)
	`
	mediaImageCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."media_image" (
			"hash"          TEXT PRIMARY KEY,
			-- media metadata
			"type"          TEXT NOT NULL,
			"data" 		    BYTEA NOT NULL,
			"pos"           INTERVAL -- position in the media if thumbnail
		)
	`
	mediaInsert = `
		INSERT INTO ${"schema"}."media" (
			"bucket", "key", "type", "duration", "title", "album", "artist", "composer", "genre", "track", "disc", "year", "description", "meta"
		) VALUES (
		 	@bucket, @key, @type, @duration, @title, @album, @artist, @composer, @genre, @track, @disc, @year, @description, @meta
		) ON CONFLICT ("bucket", "key") DO UPDATE SET
			"type" = EXCLUDED."type",
			"duration" = EXCLUDED."duration",
			"title" = EXCLUDED."title",
			"album" = EXCLUDED."album",
			"artist" = EXCLUDED."artist",
			"composer" = EXCLUDED."composer",
			"genre" = EXCLUDED."genre",
			"track" = EXCLUDED."track",
			"disc" = EXCLUDED."disc",
			"year" = EXCLUDED."year",
			"description" = EXCLUDED."description",
			"meta" = EXCLUDED."meta"			
		RETURNING 
			"bucket", "key", "type", "duration", "title", "album", "artist", "composer", "genre", "track", "disc", "year", "description", "meta"
	`
	mediaImageInsert = `
		INSERT INTO ${"schema"}."media_image" (
			"hash", "type", "data", "pos"
		) VALUES (
		 	@hash, @type, @data, @pos
		) ON CONFLICT ("hash") DO UPDATE SET
			"type" = EXCLUDED."type",
			"data" = EXCLUDED."data",
			"pos" = EXCLUDED."pos"
		RETURNING 
			"hash", "type", "data", "pos"
	`
)
