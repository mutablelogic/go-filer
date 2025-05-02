package aws_test

import (
	"context"
	"testing"

	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	assert "github.com/stretchr/testify/assert"
)

func Test_Buckets_001(t *testing.T) {
	assert := assert.New(t)
	client, err := aws.New(context.TODO())
	if assert.NoError(err) {
		assert.NotNil(client)
	} else {
		t.FailNow()
	}

	t.Run("ListBuckets_1", func(t *testing.T) {
		buckets, err := client.ListBuckets(context.TODO())
		if assert.NoError(err) {
			assert.NotNil(buckets)
			for _, bucket := range buckets {
				assert.NotNil(bucket)
				assert.NotEmpty(bucket.Name)
				assert.NotEmpty(bucket.CreationDate)
				t.Logf("Bucket: %s (%s)", *bucket.Name, bucket.CreationDate)
			}
		}
	})

	t.Run("ListBuckets_2", func(t *testing.T) {
		buckets, err := client.ListBuckets(context.TODO())
		if assert.NoError(err) {
			assert.NotNil(buckets)
			for _, bucket := range buckets {
				assert.NotNil(bucket)
				assert.NotEmpty(bucket.Name)
				assert.NotEmpty(bucket.CreationDate)
				t.Logf("Bucket: %s (%s)", *bucket.Name, bucket.CreationDate)
			}
		}
	})
}
