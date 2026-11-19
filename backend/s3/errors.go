package s3

import (
	"context"
	"errors"
	"io/fs"
	"net/http"

	// Packages
	smithyhttp "github.com/aws/smithy-go/transport/http"
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// objectError returns the error of a request for an object, or a directory,
// whose path is relative to the prefix, by its path: one which doesn't exist
// is fs.ErrNotExist, as for a filesystem, one which can't be accessed is
// ErrForbidden, one whose condition fails, as it changed, is
// ErrPreconditionFailed, one which conflicts with another change is
// ErrConflict, and one which fails otherwise is ErrServiceUnavailable. A
// request which is cancelled is its context's error.
func (self *S3) objectError(op, rel string, err error) error {
	var responseErr *smithyhttp.ResponseError
	if err == nil {
		return nil
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	} else if !errors.As(err, &responseErr) {
		return gofiler.ErrServiceUnavailable.Withf("%s /%s: %v", op, rel, err)
	}
	switch status := responseErr.HTTPStatusCode(); status {
	case http.StatusNotFound:
		return &fs.PathError{Op: op, Path: "/" + rel, Err: fs.ErrNotExist}
	case http.StatusPreconditionFailed:
		return gofiler.ErrPreconditionFailed.Withf("%s /%s: it changed since it was returned", op, rel)
	case http.StatusConflict:
		return gofiler.ErrConflict.Withf("%s /%s: it was changed at the same time", op, rel)
	case http.StatusForbidden, http.StatusUnauthorized:
		return gofiler.ErrForbidden.Withf("%s /%s: access denied", op, rel)
	default:
		return gofiler.ErrServiceUnavailable.Withf("%s /%s: %s", op, rel, http.StatusText(status))
	}
}

// isNotExist returns true when the error is of an object which doesn't exist
func isNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}
