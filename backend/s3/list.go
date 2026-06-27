package s3

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	mime "github.com/mutablelogic/go-filer/metadata/mime"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type s3ListToken struct {
	ContinuationToken *string
	SeenDirs          map[string]bool // populated when synthesizing dirs from a recursive flat listing
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Iterate through the list of objects in the backend, until io.EOF is returned.
func (self *S3Backend) ListObjects(ctx context.Context, iterator *schema.ObjectListIterator) (err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "s3.ListObjects",
		attribute.String("req", types.Stringify(iterator)),
	)
	defer func() {
		if errors.Is(err, io.EOF) {
			endSpan(nil)
		} else {
			endSpan(err)
		}
	}()

	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return err
	}

	tok, ok := iterator.Token.(*s3ListToken)
	if tok == nil || !ok {
		tok = &s3ListToken{}
		iterator.Token = tok
	}
	iterator.Body = make([]*schema.Object, 0, schema.ObjectListLimit)

	// Derive the S3 listing prefix from the backend's base path and the requested path.
	basePrefix := strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	reqPath := strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(types.Value(iterator.Path)), "/"), "/")

	var listPrefix string
	switch {
	case basePrefix != "" && reqPath != "":
		listPrefix = basePrefix + "/" + reqPath + "/"
	case basePrefix != "":
		listPrefix = basePrefix + "/"
	case reqPath != "":
		listPrefix = reqPath + "/"
	}

	// Non-recursive listing uses a delimiter to get virtual subdirectories.
	var delimiter string
	if !iterator.Recursive {
		delimiter = "/"
	}

	input := &s3svc.ListObjectsV2Input{
		Bucket:  aws.String(self.url.Host),
		MaxKeys: aws.Int32(schema.ObjectListLimit),
	}
	if listPrefix != "" {
		input.Prefix = aws.String(listPrefix)
	}
	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}
	if tok.ContinuationToken != nil {
		input.ContinuationToken = tok.ContinuationToken
	}

	out, err := self.client.ListObjectsV2(ctx, input)
	if err != nil {
		return err
	}

	listingDirs := types.Value(iterator.Type) == schema.ContentTypeDirectory
	if listingDirs && !iterator.Recursive {
		// Non-recursive: S3 returns virtual directories as CommonPrefixes.
		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			iterator.Body = append(iterator.Body, &schema.Object{
				ObjectKey: schema.ObjectKey{
					Volume: self.Name(),
					Path:   s3PathFromKey(aws.ToString(cp.Prefix), basePrefix),
				},
				ObjectMeta: schema.ObjectMeta{
					ContentType: schema.ContentTypeDirectory,
				},
			})
		}
	} else if listingDirs && iterator.Recursive {
		// Recursive: no delimiter means no CommonPrefixes; synthesize dir paths from object keys.
		if tok.SeenDirs == nil {
			tok.SeenDirs = make(map[string]bool)
		}
		for _, item := range out.Contents {
			relPath := s3PathFromKey(aws.ToString(item.Key), basePrefix)
			parts := strings.Split(relPath, "/")
			for i := 1; i < len(parts); i++ {
				dir := strings.Join(parts[:i], "/")
				if dir == reqPath || tok.SeenDirs[dir] {
					continue
				}
				tok.SeenDirs[dir] = true
				iterator.Body = append(iterator.Body, &schema.Object{
					ObjectKey: schema.ObjectKey{
						Volume: self.Name(),
						Path:   dir,
					},
					ObjectMeta: schema.ObjectMeta{
						ContentType: schema.ContentTypeDirectory,
					},
				})
			}
		}
	} else {
		for _, item := range out.Contents {
			key := aws.ToString(item.Key)
			if key == listPrefix {
				// Skip directory placeholder objects emitted by some backends.
				continue
			}
			objPath := s3PathFromKey(key, basePrefix)
			obj := &schema.Object{
				ObjectKey: schema.ObjectKey{
					Volume: self.Name(),
					Path:   objPath,
				},
				ObjectMeta: schema.ObjectMeta{
					ContentType: mime.TypeByExtension(path.Ext(objPath)),
				},
				ObjectAttr: schema.ObjectAttr{
					Size: aws.ToInt64(item.Size),
					ETag: stripETagQuotes(item.ETag),
				},
			}
			if item.LastModified != nil {
				obj.ModTime = *item.LastModified
			}
			iterator.Body = append(iterator.Body, obj)
		}
	}

	if aws.ToBool(out.IsTruncated) {
		tok.ContinuationToken = out.NextContinuationToken
		return nil
	}
	iterator.Token = nil
	return io.EOF
}

// stripETagQuotes removes surrounding double-quotes from an S3 ETag value.
func stripETagQuotes(etag *string) *string {
	if etag == nil {
		return nil
	}
	v := strings.Trim(*etag, `"`)
	return &v
}

// s3PathFromKey converts an S3 object key to a path relative to the backend root
// by stripping the base prefix and normalizing trailing slashes.
func s3PathFromKey(key, basePrefix string) string {
	if basePrefix != "" {
		key = strings.TrimPrefix(key, basePrefix+"/")
	}
	return strings.TrimSuffix(key, "/")
}
