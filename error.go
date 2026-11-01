package filer

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"

	// Packages
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Errors
type Err int

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	ErrSuccess Err = iota
	ErrNotFound
	ErrBadParameter
	ErrNotImplemented
	ErrConflict
	ErrInternalServerError
	ErrServiceUnavailable
	ErrForbidden
	ErrNotModified
	ErrCreated              // not a failure: the object was created, rather than replaced
	ErrPreconditionFailed   // a condition of the request isn't met, such as replacing an object which doesn't exist
	ErrUnprocessableContent // the request can't be processed, such as creating an object whose name is skipped
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e Err) Error() string {
	switch e {
	case ErrSuccess:
		return "success"
	case ErrNotFound:
		return "not found"
	case ErrBadParameter:
		return "bad parameter"
	case ErrNotImplemented:
		return "not implemented"
	case ErrConflict:
		return "conflict"
	case ErrInternalServerError:
		return "internal server error"
	case ErrServiceUnavailable:
		return "service unavailable"
	case ErrForbidden:
		return "forbidden"
	case ErrNotModified:
		return "not modified"
	case ErrCreated:
		return "created"
	case ErrPreconditionFailed:
		return "precondition failed"
	case ErrUnprocessableContent:
		return "unprocessable content"
	}
	return fmt.Sprintf("error code %d", int(e))
}

func (e Err) With(args ...any) error {
	return fmt.Errorf("%w: %s", e, fmt.Sprint(args...))
}

// Withf returns an error wrapping the error code, with a formatted message.
// As with fmt.Errorf, the format may use %w to wrap errors, so that they can
// still be matched with errors.Is and errors.As.
func (e Err) Withf(format string, args ...any) error {
	return fmt.Errorf("%w: %w", e, fmt.Errorf(format, args...))
}

func (e Err) HTTP() httpresponse.Err {
	switch e {
	case ErrNotFound:
		return httpresponse.ErrNotFound
	case ErrBadParameter:
		return httpresponse.ErrBadRequest
	case ErrConflict:
		return httpresponse.ErrConflict
	case ErrNotImplemented:
		return httpresponse.ErrNotImplemented
	case ErrInternalServerError:
		return httpresponse.ErrInternalError
	case ErrServiceUnavailable:
		return httpresponse.ErrServiceUnavailable
	case ErrForbidden:
		return httpresponse.ErrForbidden
	case ErrNotModified:
		return httpresponse.Err(http.StatusNotModified)
	case ErrCreated:
		return httpresponse.Err(http.StatusCreated)
	case ErrPreconditionFailed:
		return httpresponse.Err(http.StatusPreconditionFailed)
	case ErrUnprocessableContent:
		return httpresponse.Err(http.StatusUnprocessableEntity)
	default:
		return httpresponse.ErrInternalError
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// HTTPErr translates an error into an error wrapping a HTTP status code
func HTTPErr(err error) error {
	if err == nil {
		return nil
	}

	// Already a HTTP error
	var code httpresponse.Err
	if errors.As(err, &code) {
		return err
	}

	// A filer error
	var e Err
	if errors.As(err, &e) {
		return e.HTTP().Withf("%w", err)
	}

	// A request whose body ends before it should, such as an upload which
	// is interrupted, is a bad request
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return httpresponse.ErrBadRequest.Withf("%w", err)
	}

	// A filesystem error
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return httpresponse.ErrNotFound.Withf("%w", err)
	case errors.Is(err, fs.ErrExist):
		return httpresponse.ErrConflict.Withf("%w", err)
	case errors.Is(err, fs.ErrPermission):
		return httpresponse.ErrForbidden.Withf("%w", err)
	case errors.Is(err, fs.ErrInvalid):
		return httpresponse.ErrBadRequest.Withf("%w", err)
	case errors.Is(err, fs.ErrClosed):
		return httpresponse.ErrServiceUnavailable.Withf("%w", err)
	}

	// Any other error
	return httpresponse.ErrInternalError.Withf("%w", err)
}
