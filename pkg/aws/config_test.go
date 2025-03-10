package aws_test

import (
	"context"
	"testing"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/aws"
	"github.com/stretchr/testify/assert"
)

func Test_config_001(t *testing.T) {
	assert := assert.New(t)
	config := aws.Config{}
	assert.Equal("aws", config.Name())
	assert.NotEmpty(config.Description())
}

func Test_config_002(t *testing.T) {
	assert := assert.New(t)
	config := aws.Config{}
	task, err := config.New(context.TODO())
	if assert.NoError(err) {
		assert.NotNil(task)
	}
}
