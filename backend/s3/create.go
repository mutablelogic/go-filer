package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Create object in the backend
func (self *S3Backend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	basePrefix := strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	key := s3KeyFromPath(req.Path, basePrefix)
	if req.IfNotExists {
		_, err := self.client.HeadObject(ctx, &s3svc.HeadObjectInput{
			Bucket: types.Ptr(self.url.Host),
			Key:    types.Ptr(key),
		})
		if err == nil {
			return nil, gofiler.ErrConflict.Withf("object already exists: %q", req.Path)
		}
		if !s3IsNotFound(err) {
			return nil, err
		}
	}

	metadata := make(map[string]string, len(req.Meta)+1)
	for _, item := range req.Meta {
		key := strings.TrimSpace(item.Key)
		if key == "" || item.Value == nil {
			continue
		}
		var value string
		if err := json.Unmarshal(item.Value, &value); err == nil {
			metadata[key] = value
		} else {
			metadata[key] = string(item.Value)
		}
	}
	if !req.ModTime.IsZero() {
		metadata[schema.AttrLastModified] = req.ModTime.Format(time.RFC3339)
	}

	body := req.Body
	if body == nil {
		body = bytes.NewReader(nil)
	}
	input := &s3svc.PutObjectInput{
		Bucket: types.Ptr(self.url.Host),
		Key:    types.Ptr(key),
		Body:   body,
	}
	if req.ContentType != "" {
		input.ContentType = aws.String(req.ContentType)
	}
	if len(metadata) != 0 {
		input.Metadata = metadata
	}

	uploader := s3manager.NewUploader(self.client, func(u *s3manager.Uploader) {
		u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	output, err := uploader.Upload(ctx, input)
	if err != nil {
		return nil, err
	}
	obj := &schema.Object{
		ObjectKey: req.ObjectKey,
		ObjectMeta: schema.ObjectMeta{
			ContentType: req.ContentType,
			Meta:        req.Meta,
		},
		ObjectAttr: schema.ObjectAttr{
			ETag:    stripETagQuotes(output.ETag),
			ModTime: req.ModTime,
		},
	}
	return obj, nil
}
