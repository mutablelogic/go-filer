package s3

import (
	"errors"
	"net/http"
	"testing"

	// Packages
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
)

// An error of a status which isn't mapped is unavailable, with its status
// and cause, even of a status which has no text
func Test_ObjectErrorUnmapped(t *testing.T) {
	b := new(S3)
	for _, status := range []int{http.StatusServiceUnavailable, 599} {
		err := b.objectError("read", "a.txt", &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("SlowDown: please reduce your request rate"),
		})
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
		assert.Contains(t, err.Error(), "read /a.txt")
		assert.Contains(t, err.Error(), "SlowDown")
		assert.Contains(t, err.Error(), http.StatusText(status))
	}
}
