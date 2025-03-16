package aws

import (
	"errors"

	// Packages
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Transform the error into an httpresponse error
func Err(err error) error {
	var awserr *awshttp.ResponseError
	if errors.As(err, &awserr) {
		return httpresponse.Err(awserr.HTTPStatusCode()).With(awserr.Error())
	}
	return err
}
