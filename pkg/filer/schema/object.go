package schema

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	// Packages
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	pg "github.com/djthorpe/go-pg"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectMeta struct {
	Key  string `json:"key,omitempty" name:"key" help:"Object key"`
	Type string `json:"type,omitempty" name:"type" help:"Type of the object"`
}

type Object struct {
	ObjectMeta
	Hash   *string   `json:"hash,omitempty" name:"hash" help:"Hash of the object"`
	Bucket string    `json:"bucket,omitempty" name:"bucket" help:"Bucket in which the object is stored"`
	Size   int64     `json:"size" name:"size" help:"Size of the object in bytes"`
	Ts     time.Time `json:"ts,omitzero" name:"ts" help:"Creation date of the object"`
}

type ObjectList struct {
	// TODO: We don't support a count yet
	Body []*Object `json:"body,omitempty" name:"body" help:"List of objects"`
}

type ObjectListRequest struct {
	Prefix *string `json:"prefix,omitempty" name:"prefix" help:"Prefix of the object key"`
	pg.OffsetLimit
}

////////////////////////////////////////////////////////////////////////////////
// GLOBAL VARIABLES

const (
	PathSeparator = string('/')
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func ObjectFromAWS(object *s3types.Object, bucket string, meta url.Values) *Object {
	return &Object{
		ObjectMeta: ObjectMeta{
			Key:  types.PtrString(object.Key),
			Type: meta.Get(strings.ToLower(types.ContentTypeHeader)),
		},
		Hash:   unquote(object.ETag),
		Bucket: bucket,
		Size:   types.PtrInt64(object.Size),
		Ts:     types.PtrTime(object.LastModified),
	}
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectMeta) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectList) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectListRequest) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (o *Object) Scan(row pg.Row) error {
	return row.Scan(&o.Bucket, &o.Key, &o.Type, &o.Hash, &o.Size, &o.Ts)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (o Object) Insert(bind *pg.Bind) (string, error) {
	if o.Bucket == "" {
		return "", httpresponse.ErrBadRequest.With("missing bucket")
	} else if o.Key == "" {
		return "", httpresponse.ErrBadRequest.With("missing key")
	} else if o.Type == "" {
		return "", httpresponse.ErrBadRequest.With("missing type")
	}

	// Bind
	bind.Set("bucket", o.Bucket)
	bind.Set("key", o.Key)
	bind.Set("type", o.Type)
	bind.Set("hash", o.Hash)
	bind.Set("size", o.Size)
	bind.Set("ts", o.Ts)

	// Return success
	return objectInsert, nil
}

func (o Object) Update(bind *pg.Bind) error {
	return httpresponse.ErrNotImplemented.With("Object.Update not implemented")
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func unquote(s *string) *string {
	if s == nil {
		return nil
	}
	return types.StringPtr(types.Unquote(*s))
}

////////////////////////////////////////////////////////////////////////////////
// SQL

// Create objects in the schema
func bootstrapObject(ctx context.Context, conn pg.Conn) error {
	q := []string{
		objectCreateTable,
		objectCreateTriggerFunction,
		objectCreateTrigger,
		objectMediaCreateTable,
	}
	for _, query := range q {
		if err := conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

const (
	objectCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."object" (
			"bucket"        TEXT NOT NULL,
			"key"           TEXT NOT NULL,
			-- object metadata
			"type"          TEXT NOT NULL,
			"hash"          TEXT,
			"size"          BIGINT NOT NULL,
			"ts"            TIMESTAMP WITH TIME ZONE,
			-- primary key
			PRIMARY KEY ("bucket", "key")
		)
	`
	objectMediaCreateTable = `
		CREATE TABLE IF NOT EXISTS ${"schema"}."media" (
			"bucket"        TEXT NOT NULL,
			"key"           TEXT NOT NULL,
			-- object media metadata
			"duration"      INTERVAL,
			"meta"          JSONB DEFAULT '{}'::JSONB,
			-- foreign key
			FOREIGN KEY ("bucket", "key") REFERENCES ${"schema"}."object" ("bucket", "key") ON DELETE CASCADE
		)
	`
	objectCreateTriggerFunction = `
		CREATE OR REPLACE FUNCTION ${"schema"}.object_create() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM ${"pgqueue_schema"}.queue_insert(${'ns'}, ${'queue_registerobject'}, row_to_json(NEW)::JSONB, NULL);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`
	objectCreateTrigger = `
		CREATE OR REPLACE TRIGGER 
			object_update AFTER INSERT OR UPDATE ON ${"schema"}."object"
		FOR EACH ROW EXECUTE PROCEDURE
			${"schema"}.object_create() 
	`
	objectInsert = `
		INSERT INTO ${"schema"}."object" (
			"bucket", "key", "type", "hash", "size", "ts"
		) VALUES (
		 	@bucket, @key, @type, @hash, @size, @ts
		) ON CONFLICT (bucket,key) DO UPDATE SET
		 	type = EXCLUDED.type,
			hash = EXCLUDED.hash,
			size = EXCLUDED.size,
			ts = EXCLUDED.ts 
		RETURNING 
			"bucket", "key", "type", "hash", "size", "ts"
	`
	objectMediaInsert = `
		INSERT INTO ${"schema"}."media" (
			"bucket", "key", "duration", "meta"
		) VALUES (
		 	@bucket, @key, @duration, @meta
		) RETURNING 
			"bucket", "key", "duration", "meta"
	`
)
