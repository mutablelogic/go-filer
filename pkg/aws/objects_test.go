package aws_test

import (
	"context"
	"testing"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/aws"
	"github.com/stretchr/testify/assert"
)

func Test_Objects_001(t *testing.T) {
	assert := assert.New(t)
	client, err := aws.New(context.TODO())
	if assert.NoError(err) {
		assert.NotNil(client)
	} else {
		t.FailNow()
	}

	t.Run("ListObjects_1", func(t *testing.T) {
		buckets, err := client.ListBuckets(context.TODO())
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.NotNil(buckets)
		for _, bucket := range buckets {
			objects, err := client.ListObjects(context.TODO(), *bucket.Name)
			if assert.NoError(err) {
				assert.NotNil(objects)
				for _, object := range objects {
					assert.NotNil(object)
					assert.NotEmpty(object.Key)
					assert.NotEmpty(object.LastModified)
					t.Logf("Object: %s (%s)", *object.Key, object.LastModified)
				}
			}
		}
	})
}
