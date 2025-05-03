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
	Width       *uint64        `json:"width,omitempty" help:"Width (in pixels) of the media"`
	Height      *uint64        `json:"height,omitempty" help:"Height (in pixels) of the media"`
	Title       *string        `json:"title,omitempty" help:"Title of the media"`
	Album       *string        `json:"album,omitempty" help:"Album of the media"`
	Artist      *string        `json:"artist,omitempty" help:"Artist of the media"`
	Composer    *string        `json:"composer,omitempty" help:"Composer of the media"`
	Genre       *string        `json:"genre,omitempty" help:"Genre of the media"`
	Track       *uint64        `json:"track,omitempty" help:"Track number of the media"`
	Disc        *uint64        `json:"disc,omitempty" help:"Disc number of the media"`
	Count       *uint64        `json:"count,omitempty" help:"Number of pages or files in the media"`
	Year        *uint64        `json:"year,omitempty" help:"Year of the media"`
	Description *string        `json:"description,omitempty" help:"Synopsis"`
	Ts          *time.Time     `json:"ts,omitempty" help:"Creation date of the media"`
	Meta        map[string]any `json:"meta" help:"Media metadata"`
	Images      []MediaImage   `json:"images,omitempty" help:"Artwork and thumbnails"`
}

type Media struct {
	Bucket string `json:"bucket" help:"Bucket in which the object is stored"`
	Key    string `json:"key" help:"Object key"`
	MediaMeta
}

type MediaImage struct {
	Type string         `json:"type" help:"Type of the media"`
	Hash string         `json:"hash" help:"Hash of the media data"`
	Data []byte         `json:"data" help:"Media data"`
	Pos  *time.Duration `json:"pos,omitempty" help:"Position in the media if thumbnail"`
}

type MediaImageMap struct {
	Bucket string `json:"bucket" help:"Bucket in which the object is stored"`
	Key    string `json:"key" help:"Object key"`
	Hash   string `json:"hash" help:"Hash of the media data"`
}

type MediaFragmentMeta struct {
	Type  string         `json:"type" help:"Type of the fragment"`
	Text  string         `json:"text" help:"Text of the fragment"`
	Index *uint64        `json:"index" help:"Index of the fragment (page number, etc)"`
	Start *time.Duration `json:"start,omitempty" help:"Start interval of the fragment, if subtitles"`
	End   *time.Duration `json:"end,omitempty" help:"End interval of the fragment, if subtitles"`
}

type MediaFragment struct {
	Bucket string `json:"bucket" help:"Bucket in which the object is stored"`
	Key    string `json:"key" help:"Object key"`
	MediaFragmentMeta
}

type MediaFragmentList []MediaFragment

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

func (m MediaImageMap) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m MediaFragment) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m MediaFragmentMeta) String() string {
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
// SELECTOR

func (m MediaImageMap) Select(bind *pg.Bind, op pg.Op) (string, error) {
	var where []string

	if m.Bucket != "" {
		where = append(where, `"bucket" = `+bind.Set("bucket", m.Bucket))
	}
	if m.Key != "" {
		where = append(where, `"key" = `+bind.Set("key", m.Key))
	}
	if m.Hash != "" {
		where = append(where, `"hash" = `+bind.Set("hash", m.Hash))
	}
	if len(where) > 0 {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	} else {
		return "", httpresponse.ErrBadRequest.With("MediaImageMap.Select: no where clause")
	}

	switch op {
	case pg.Delete:
		return mediaImageMapDelete, nil
	default:
		return "", httpresponse.ErrBadRequest.Withf("MediaImageMap.Select: unsupported operation %q", op)
	}
}

func (m MediaFragmentMeta) Select(bind *pg.Bind, op pg.Op) (string, error) {
	var where []string

	if bind.Has("bucket") {
		where = append(where, `"bucket" = @bucket`)
	}
	if bind.Has("key") {
		where = append(where, `"key" = @key`)
	}
	if m.Type != "" {
		where = append(where, `"type" = `+bind.Set("type", m.Type))
	}
	if len(where) > 0 {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	} else {
		return "", httpresponse.ErrBadRequest.With("MediaFragment.Select: no where clause")
	}

	switch op {
	case pg.Delete:
		return mediaFragmentDelete, nil
	default:
		return "", httpresponse.ErrBadRequest.Withf("MediaFragment.Select: unsupported operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (m *Media) Scan(row pg.Row) error {
	return row.Scan(&m.Bucket, &m.Key, &m.Type, &m.Duration, &m.Width, &m.Height, &m.Title, &m.Album, &m.Artist, &m.Composer, &m.Genre, &m.Track, &m.Disc, &m.Count, &m.Year, &m.Description, &m.Ts, &m.Meta)
}

func (m *MediaImage) Scan(row pg.Row) error {
	return row.Scan(&m.Hash, &m.Type, &m.Data, &m.Pos)
}

func (m *MediaImageMap) Scan(row pg.Row) error {
	return row.Scan(&m.Bucket, &m.Key, &m.Hash)
}

func (m *MediaFragment) Scan(row pg.Row) error {
	return row.Scan(&m.Bucket, &m.Key, &m.Type, &m.Text, &m.Index, &m.Start, &m.End)
}

func (m *MediaFragmentList) Scan(row pg.Row) error {
	var fragment MediaFragment
	if m == nil {
		*m = make(MediaFragmentList, 0)
	}
	if err := fragment.Scan(row); err != nil {
		return err
	} else {
		*m = append(*m, fragment)
	}
	return nil
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
	bind.Set("width", m.Width)
	bind.Set("height", m.Height)
	bind.Set("meta", m.Meta)
	bind.Set("title", m.Title)
	bind.Set("album", m.Album)
	bind.Set("artist", m.Artist)
	bind.Set("composer", m.Composer)
	bind.Set("genre", m.Genre)
	bind.Set("track", m.Track)
	bind.Set("disc", m.Disc)
	bind.Set("count", m.Count)
	bind.Set("year", m.Year)
	bind.Set("description", m.Description)
	bind.Set("ts", m.Ts)

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

func (m MediaImageMap) Insert(bind *pg.Bind) (string, error) {
	// Check parameters
	if m.Hash == "" {
		return "", httpresponse.ErrBadRequest.With("missing hash")
	} else if m.Bucket == "" {
		return "", httpresponse.ErrBadRequest.With("missing bucket")
	} else if m.Key == "" {
		return "", httpresponse.ErrBadRequest.With("missing key")
	}

	// Bind the metadata
	bind.Set("hash", m.Hash)
	bind.Set("bucket", m.Bucket)
	bind.Set("key", m.Key)

	// Return success
	return mediaImageMapInsert, nil
}

func (m MediaFragmentMeta) Insert(bind *pg.Bind) (string, error) {
	// Check bind for bucket and key
	if !bind.Has("bucket") {
		return "", httpresponse.ErrBadRequest.With("missing bucket")
	} else if !bind.Has("key") {
		return "", httpresponse.ErrBadRequest.With("missing key")
	} else if m.Type == "" {
		return "", httpresponse.ErrBadRequest.With("missing type")
	}

	// Bind the metadata
	bind.Set("type", m.Type)
	bind.Set("text", m.Text)
	bind.Set("index", m.Index)
	bind.Set("start", m.Start)
	bind.Set("end", m.End)

	// Return success
	return mediaFragmentInsert, nil
}

func (m MediaMeta) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaMeta.Update not implemented")
}

func (m MediaImage) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaImage.Update not implemented")
}

func (m MediaImageMap) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaImageMap.Update not implemented")
}

func (m MediaFragmentMeta) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("MediaFragmentMeta.Update not implemented")
}

////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapMedia(ctx context.Context, conn pg.Conn) error {
	q := []string{
		mediaCreateTable,
		mediaImageCreateTable,
		mediaImageMapCreateTable,
		mediaFragmentCreateTable,
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
			"width" 	    INTEGER,
			"height"        INTEGER,
			"title"		 	TEXT,
			"album"         TEXT,
			"artist"        TEXT, -- also director
			"composer"	  	TEXT,
			"genre"         TEXT,
			"track"		    INTEGER, -- also episode
			"disc"		    INTEGER, -- also season
			"count"		    INTEGER, -- number of pages or files
			"year"		    INTEGER,
			"description"   TEXT,
			"ts"			TIMESTAMP, -- creation date
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
	mediaImageMapCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."media_image_map" (
			"hash"          TEXT NOT NULL,
			"bucket"        TEXT NOT NULL,
			"key"           TEXT NOT NULL,
			-- foreign keys
			FOREIGN KEY ("bucket", "key") REFERENCES ${"schema"}."object" ("bucket", "key") ON DELETE CASCADE,
			FOREIGN KEY ("hash") REFERENCES ${"schema"}."media_image" ("hash") ON DELETE CASCADE
		)
	`
	mediaFragmentCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."media_fragment" (
			"bucket"        TEXT NOT NULL,
			"key"           TEXT NOT NULL,
			-- text fragment
			"type"          TEXT NOT NULL,
			"text" 		    TEXT NOT NULL,
			"index"		    INTEGER,   -- index of the fragment
			"start"         INTERVAL,  -- start interval of the fragment, if subtitles
			"end"		    INTERVAL,  -- end interval of the fragment, if subtitles
			-- foreign keys
			FOREIGN KEY ("bucket", "key") REFERENCES ${"schema"}."object" ("bucket", "key") ON DELETE CASCADE
		)
	`
	mediaInsert = `
		INSERT INTO ${"schema"}."media" (
			"bucket", "key", "type", "duration", "width", "height", "title", "album", "artist", "composer", "genre", "track", "disc", "count", "year", "description", "ts", "meta"
		) VALUES (
		 	@bucket, @key, @type, @duration, @width, @height, @title, @album, @artist, @composer, @genre, @track, @disc, @count, @year, @description, @ts, @meta
		) ON CONFLICT ("bucket", "key") DO UPDATE SET
			"type" = EXCLUDED."type",
			"duration" = EXCLUDED."duration",
			"width" = EXCLUDED."width",
			"height" = EXCLUDED."height",
			"title" = EXCLUDED."title",
			"album" = EXCLUDED."album",
			"artist" = EXCLUDED."artist",
			"composer" = EXCLUDED."composer",
			"genre" = EXCLUDED."genre",
			"track" = EXCLUDED."track",
			"disc" = EXCLUDED."disc",
			"count" = EXCLUDED."count",
			"year" = EXCLUDED."year",
			"description" = EXCLUDED."description",
			"ts" = EXCLUDED."ts",
			"meta" = EXCLUDED."meta"			
		RETURNING 
			"bucket", "key", "type", "duration", "width", "height", "title", "album", "artist", "composer", "genre", "track", "disc", "count", "year", "description", "ts", "meta"
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
	mediaImageMapInsert = `
		INSERT INTO ${"schema"}."media_image_map" (
			"bucket", "key", "hash"
		) VALUES (
		 	@bucket, @key, @hash
		) RETURNING 
			"bucket", "key", "hash"
	`
	mediaImageMapDelete = `
		DELETE FROM ${"schema"}."media_image_map" ${where} RETURNING "bucket", "key", "hash"
	`
	mediaFragmentInsert = `
		INSERT INTO ${"schema"}."media_fragment" (
			"bucket", "key", "type", "text", "index", "start", "end"
		) VALUES (
		 	@bucket, @key, @type, @text, @index, @start, @end
		) RETURNING
			"bucket", "key", "type", "text", "index", "start", "end"
	`
	mediaFragmentDelete = `
		DELETE FROM ${"schema"}."media_fragment" ${where} RETURNING "bucket", "key", "type", "text", "index", "start", "end"
	`
)
