package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"

	// Packages

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Minumum part size for multipart uploads
	minPartSize = 5 * 1024 * 1024 // 5MB
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects lists all objects in an S3 bucket. Adding WithPrefix() to the
// options will limit the objects to those with a key that starts with the
// specified prefix.
// TODO: up to the specified limit.
func (aws *Client) ListObjects(ctx context.Context, bucket string, opts ...Opt) ([]s3types.Object, error) {
	var result []s3types.Object

	// Parse options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Iterate through the objects
	if err := listObjects(ctx, aws.S3(), bucket, opt.prefix, func(objects []s3types.Object) error {
		result = append(result, objects...)
		return nil
	}); err != nil {
		return nil, err
	}

	// Return the list of objects
	return result, nil
}

// GetObjectMeta returns the object metadata with the specified key in the
// specified bucket. The object is not downloaded.
func (aws *Client) GetObjectMeta(ctx context.Context, bucket, key string) (*s3types.Object, url.Values, error) {
	// Get the object metadata
	result, err := aws.S3().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return nil, nil, Err(err)
	}

	// Convert the metadata to a url.Values
	metadata := make(url.Values)
	for k, v := range result.Metadata {
		metadata.Set(k, v)
	}

	// Return the object metadata
	return &s3types.Object{
		Key:          types.StringPtr(key),
		ETag:         result.ETag,
		LastModified: result.LastModified,
		Size:         result.ContentLength,
	}, metadata, nil
}

// DeleteObject deletes the object with the specified key in the specified
// bucket.
func (aws *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := aws.S3().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return Err(err)
	}
	_, err = aws.S3().DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return Err(err)
	}

	// Return success
	return nil
}

// PutObject creates or updates an object in the specified bucket with the specified
// key. The object is created from the specified reader. Content Type and additional
// metadata can be specified in the options.
func (aws *Client) PutObject(ctx context.Context, bucket, key string, r io.Reader, opts ...Opt) (*s3types.Object, error) {
	// Parse options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Create a multipart uploader
	uploader, err := aws.S3().CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:             types.StringPtr(bucket),
		Key:                types.StringPtr(key),
		ContentType:        opt.contentType,
		ContentDisposition: types.StringPtr(fmt.Sprintf("inline; filename=%q", filepath.Base(key))),
		Metadata:           opt.metadata,
	})
	if err != nil {
		return nil, Err(err)
	}

	var completedParts []s3types.CompletedPart
	var partNumber int32
	var size int64
	buf := make([]byte, minPartSize)
	for {
		partNumber++
		part, n, err := uploadPart(ctx, r, aws.S3(), bucket, key, types.PtrString(uploader.UploadId), partNumber, buf)
		if err != nil && !errors.Is(err, io.EOF) {
			// Abort the multipart upload
			_, err2 := aws.S3().AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   types.StringPtr(bucket),
				Key:      types.StringPtr(key),
				UploadId: uploader.UploadId,
			})
			return nil, errors.Join(err, Err(err2))
		}

		// Append the completed part
		if part != nil {
			completedParts = append(completedParts, *part)
		}

		// Update the size
		size += int64(n)

		// Return uploaded objects
		if errors.Is(err, io.EOF) {
			break
		}
	}

	// Complete the multipart upload
	result, err := aws.S3().CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   types.StringPtr(bucket),
		Key:      types.StringPtr(key),
		UploadId: uploader.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return nil, Err(err)
	}

	// Return success
	return &s3types.Object{
		Key:  result.Key,
		ETag: result.ETag,
		Size: types.Int64Ptr(size),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func uploadPart(ctx context.Context, r io.Reader, client *s3.Client, bucket, key, uploadId string, partNumber int32, buf []byte) (*s3types.CompletedPart, int, error) {
	// Read until the buffer is full or EOF
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, 0, err
	}

	// Translate the error to EOF
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}

	// If n is zero, then NOP
	if n == 0 {
		return nil, 0, io.EOF
	}

	// Upload the part
	uploadResult, err2 := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     types.StringPtr(bucket),
		Key:        types.StringPtr(key),
		UploadId:   types.StringPtr(uploadId),
		Body:       bytes.NewReader(buf[:n]),
		PartNumber: types.Int32Ptr(partNumber),
	})
	if err2 != nil {
		return nil, 0, err2
	}

	// Return the completed part
	return &s3types.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: types.Int32Ptr(partNumber),
	}, n, err
}

func listObjects(ctx context.Context, client *s3.Client, bucket string, prefix *string, fn func(objects []s3types.Object) error) error {
	var token *string
	for {
		// List objects
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            types.StringPtr(bucket),
			Prefix:            prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return Err(err)
		}

		// Return objects
		if err := fn(objects.Contents); err != nil {
			return err
		}

		// Check if there are more objects to list
		if objects.NextContinuationToken == nil {
			break
		} else {
			token = objects.NextContinuationToken
		}
	}

	// Return success
	return nil
}
