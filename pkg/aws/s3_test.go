package aws_test

import (
	"context"
	"testing"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/aws"
	"github.com/stretchr/testify/assert"
)

func Test_s3_001(t *testing.T) {
	assert := assert.New(t)
	config := aws.Config{}
	task, err := config.New(context.TODO())
	if assert.NoError(err) {
		assert.NotNil(task)
	}

	t.Run("ListBuckets", func(t *testing.T) {
		s3 := task.(aws.AWSTask).S3()
		assert.NotNil(s3)

		buckets, err := s3.ListBuckets(context.TODO(), nil)
		if assert.NoError(err) {
			assert.NotNil(buckets)
		}
	})
}
