package aws

import (
	"context"
	"strings"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

const s3DeleteBatchSize = 1000

// DeleteObject deletes a single object. Returns ErrNotFound if the object
// does not exist.
func (b *backend) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	ref := b.Name() + ":" + objPath
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	// HeadObject to retrieve metadata for the response and verify existence.
	head, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, gofiler.ErrNotFound.Withf("object %q not found", ref)
		}
		return nil, s3Err(err, ref)
	}

	if _, err := b.client.DeleteObject(ctx, &s3svc.DeleteObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
	}); err != nil {
		return nil, s3Err(err, ref)
	}

	return b.attrsFromHead(objPath, head), nil
}

// DeleteObjects deletes objects matching a path or prefix.
// If an exact object exists at the path, only that object is deleted.
// Otherwise the path is treated as a prefix; use Recursive=true to delete
// nested objects.
// Uses S3 batch delete (up to 1000 keys per request) for efficiency.
func (b *backend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	response := schema.DeleteObjectsResponse{Volume: b.Name()}
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	// Check whether the path is a single real object.
	if sk != "" {
		_, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
			Bucket: awssdk.String(b.bucket),
			Key:    awssdk.String(sk),
		})
		if err == nil {
			obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{Path: req.Path})
			if err != nil {
				return nil, err
			}
			if obj != nil {
				response.Body = append(response.Body, *obj)
			}
			return &response, nil
		}
		if !isNotFound(err) {
			return nil, s3Err(err, b.Name()+":"+objPath)
		}
	}

	// Treat as prefix — list then batch delete.
	prefix := strings.TrimSuffix(sk, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	var delim string
	if !req.Recursive {
		delim = "/"
	}

	// Collect all keys and their object info from the listing.
	var keys []string
	var objs []schema.Object
	var token *string
	for {
		input := &s3svc.ListObjectsV2Input{
			Bucket: awssdk.String(b.bucket),
		}
		if prefix != "" {
			input.Prefix = awssdk.String(prefix)
		}
		if delim != "" {
			input.Delimiter = awssdk.String(delim)
		}
		if token != nil {
			input.ContinuationToken = token
		}

		out, err := b.client.ListObjectsV2(ctx, input)
		if err != nil {
			return &response, s3Err(err, b.Name()+":"+objPath)
		}

		for _, item := range out.Contents {
			if item.Key == nil || *item.Key == prefix {
				continue
			}
			keys = append(keys, *item.Key)
			objs = append(objs, b.attrsFromListItem(item))
		}

		if !awssdk.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	// Batch delete in chunks.
	for i := 0; i < len(keys); i += s3DeleteBatchSize {
		end := i + s3DeleteBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		ids := make([]s3types.ObjectIdentifier, end-i)
		for j, k := range keys[i:end] {
			ids[j] = s3types.ObjectIdentifier{Key: awssdk.String(k)}
		}

		delOut, err := b.client.DeleteObjects(ctx, &s3svc.DeleteObjectsInput{
			Bucket: awssdk.String(b.bucket),
			Delete: &s3types.Delete{
				Objects: ids,
				Quiet:   awssdk.Bool(true),
			},
		})
		if err != nil {
			return &response, s3Err(err, b.Name()+":"+objPath)
		}
		if len(delOut.Errors) > 0 {
			e := delOut.Errors[0]
			return &response, gofiler.ErrInternalServerError.Withf(
				"batch delete error for key %q: %s",
				awssdk.ToString(e.Key),
				awssdk.ToString(e.Message),
			)
		}

		response.Body = append(response.Body, objs[i:end]...)
	}

	return &response, nil
}
