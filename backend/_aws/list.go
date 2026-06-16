package aws

import (
	"context"
	"strings"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

// ListObjects lists objects in the backend.
// If an exact object exists at the path, it returns just that object.
// Otherwise the path is treated as a prefix and all matching objects are returned.
// Use Recursive=true to descend into subdirectories.
// Count reflects the total matches before Offset/Limit pagination is applied.
// Limit=0 returns only the count (Body is nil).
func (b *backend) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	// If the path doesn't look like an explicit directory, probe for an exact object first.
	isExplicitDir := req.Path == "" || req.Path == "/" || strings.HasSuffix(req.Path, "/")
	if !isExplicitDir && sk != "" {
		head, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
			Bucket: awssdk.String(b.bucket),
			Key:    awssdk.String(sk),
		})
		if err == nil {
			obj := b.attrsFromHead(objPath, head)
			var body []schema.Object
			if req.Limit != 0 {
				body = []schema.Object{*obj}
			}
			return &schema.ListObjectsResponse{
				Name:  b.Name(),
				Count: 1,
				Body:  body,
			}, nil
		}
		if !isNotFound(err) {
			return nil, s3Err(err, b.Name()+":"+objPath)
		}
		// Not an exact object — fall through to prefix listing.
	}

	// Compute the listing prefix.
	prefix := strings.TrimSuffix(sk, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}
	addSpanAttrs(ctx, attribute.String("s3.prefix", prefix))

	var delim string
	if !req.Recursive {
		delim = "/"
	}

	var all []schema.Object
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
			return nil, s3Err(err, b.Name()+":"+objPath)
		}

		for _, item := range out.Contents {
			// Skip the prefix placeholder itself (some backends emit it).
			if awssdk.ToString(item.Key) == prefix {
				continue
			}
			all = append(all, b.attrsFromListItem(item))
		}
		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			all = append(all, schema.Object{
				Name:  b.Name(),
				Path:  b.pathFromStorageKey(*cp.Prefix),
				IsDir: true,
			})
		}

		if !awssdk.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	// Apply Offset/Limit pagination.
	start := req.Offset
	if start < 0 {
		start = 0
	}
	if start > len(all) {
		start = len(all)
	}
	page := all[start:]
	if req.Limit > 0 && req.Limit < len(page) {
		page = page[:req.Limit]
	}
	if req.Limit == 0 {
		page = nil
	}

	return &schema.ListObjectsResponse{
		Name:  b.Name(),
		Count: len(all),
		Body:  page,
	}, nil
}
