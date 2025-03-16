package client_test

import (
	"testing"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/filer/client"
	"github.com/stretchr/testify/assert"
)

func Test_Uploader_01(t *testing.T) {
	assert := assert.New(t)

	// Create a new uploader
	u := client.NewUploader()
	assert.NotNil(u)

	// Add files
	assert.NoError(u.Add("name", "../../.././pkg"))
}
