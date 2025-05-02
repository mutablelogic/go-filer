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
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
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
func (aws *Client) ListObjects(ctx context.Context, bucket string, opts ...filer.Opt) ([]s3types.Object, error) {
	var result []s3types.Object

	// Parse options
	opt, err := filer.ApplyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Iterate through the objects
	if err := listObjects(ctx, aws.S3(), bucket, opt.Prefix(), func(objects []s3types.Object) error {
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

// WriteObject write an object to an io.Writer, expecting the caller to close
// the writer. The return value is the number of bytes written.
func (aws *Client) WriteObject(ctx context.Context, w io.Writer, bucket, key string, opts ...filer.Opt) (int64, error) {
	// Parse range option
	opt, err := filer.ApplyOpts(opts...)
	if err != nil {
		return -1, err
	}

	// Get the object data
	result, err := aws.S3().GetObject(ctx, &s3.GetObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
		Range:  opt.RangeHeader(),
	})
	if err != nil {
		return -1, Err(err)
	}

	// Copy the object data to the writer
	n, err := io.Copy(w, result.Body)

	// Close the response body
	err = errors.Join(err, result.Body.Close())

	// Return the number of bytes written and any error
	return n, err
}

// GetObject returns the metadata and writes the object data with the specified key. If w is nil, no
// data is written. If meta is nil, the metadata function is not called. The object is
// returned after the data is written.
func (aws *Client) GetObject(ctx context.Context, w io.Writer, meta func(url.Values) error, bucket, key string) (*s3types.Object, error) {
	// Get the object metadata
	result, err := aws.S3().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return nil, Err(err)
	}

	// Metadata
	if meta != nil {
		// Convert the metadata to a url.Values
		metadata := make(url.Values)
		for k, v := range result.Metadata {
			metadata.Set(k, v)
		}
		if err := meta(metadata); err != nil {
			return nil, err
		}
	}

	// Data
	if w != nil {
		// Get the object data
		result, err := aws.S3().GetObject(ctx, &s3.GetObjectInput{
			Bucket: types.StringPtr(bucket),
			Key:    types.StringPtr(key),
		})
		if err != nil {
			return nil, Err(err)
		}
		defer result.Body.Close()
		if _, err := io.Copy(w, result.Body); err != nil {
			return nil, err
		}
	}

	// Return the object metadata
	return &s3types.Object{
		Key:          types.StringPtr(key),
		ETag:         result.ETag,
		LastModified: result.LastModified,
		Size:         result.ContentLength,
	}, nil
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

// DeleteObjects deletes objects in a bucket, based on a callabck filter, and with additional
// filtering options
func (aws *Client) DeleteObjects(ctx context.Context, bucket string, fn func(*s3types.Object) bool, opts ...filer.Opt) error {
	var result []s3types.ObjectIdentifier

	// Parse options
	opt, err := filer.ApplyOpts(opts...)
	if err != nil {
		return err
	}

	// Iterate through the objects and add them to the list of objects to delete
	if err := listObjects(ctx, aws.S3(), bucket, opt.Prefix(), func(objects []s3types.Object) error {
		for _, object := range objects {
			if fn == nil || fn(&object) {
				result = append(result, s3types.ObjectIdentifier{
					Key: object.Key,
				})
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Iterate through the objects and delete them
	offset := 0
	for {
		if offset >= len(result) {
			break
		}

		// Calculate the end index for the current batch
		end := offset + schema.ObjectListLimit
		if end > len(result) {
			end = len(result)
		}

		// Get the batch slice
		batch := result[offset:end]

		// Ensure batch is not empty before calling delete
		if len(batch) == 0 {
			break // Should not happen if limit < len(result), but good practice
		}

		// Delete objects up to the specified limit
		_, err := aws.S3().DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: types.StringPtr(bucket),
			Delete: &s3types.Delete{
				Objects: batch,
				Quiet:   types.BoolPtr(true),
			},
		})
		if err != nil {
			return Err(err)
		}

		// Move the limit to the start of the next batch
		offset = end
	}

	// Return success
	return nil
}

// PutObject creates or updates an object in the specified bucket with the specified
// key. The object is created from the specified reader. Content Type and additional
// metadata can be specified in the options.
func (aws *Client) PutObject(ctx context.Context, bucket, key string, r io.Reader, opts ...filer.Opt) (*s3types.Object, error) {
	// Parse options
	opt, err := filer.ApplyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Create a multipart uploader
	uploader, err := aws.S3().CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:             types.StringPtr(bucket),
		Key:                types.StringPtr(key),
		ContentType:        opt.ContentType(),
		ContentDisposition: types.StringPtr(fmt.Sprintf("inline; filename=%q", filepath.Base(key))),
		Metadata:           opt.Meta(),
	})
	if err != nil {
		return nil, Err(err)
	}

	var completedParts []s3types.CompletedPart
	var partNumber int32
	var size int64
	var buf = make([]byte, minPartSize)
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
			return nil, errors.Join(Err(err), Err(err2))
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
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		// Translate the error to EOF
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
