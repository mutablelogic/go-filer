package gofiler

import (
	"errors"
	"fmt"
	"net/http"

	// Packages
	"github.com/mutablelogic/go-pg"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

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
	ErrNotIndexed
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Errors
type Err int

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
	case ErrNotIndexed:
		return "not indexed"
	}
	return fmt.Sprintf("error code %d", int(e))
}

func (e Err) With(args ...any) error {
	return fmt.Errorf("%w: %s", e, fmt.Sprint(args...))
}

func (e Err) Withf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", e, fmt.Sprintf(format, args...))
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
	case ErrNotIndexed:
		return httpresponse.Err(http.StatusNotModified)
	default:
		return httpresponse.ErrInternalError
	}
}

func HTTPErr(err error) error {
	if err == nil {
		return nil
	}
	// Check for http error
	var httpErr httpresponse.Err
	if errors.As(err, &httpErr) {
		return err
	}

	// Check for database error
	if pg.IsDatabaseError(err) || errors.Is(err, pg.ErrNotFound) || errors.Is(err, pg.ErrBadParameter) || errors.Is(err, pg.ErrConflict) {
		return pg.HTTPError(err)
	}

	// Check for filter error
	var schemaErr Err
	if errors.As(err, &schemaErr) {
		return schemaErr.HTTP().With(err)
	}

	switch {
	case errors.Is(err, ErrNotFound):
		return httpresponse.ErrNotFound.With(err)
	case errors.Is(err, ErrBadParameter):
		return httpresponse.ErrBadRequest.With(err)
	case errors.Is(err, ErrConflict):
		return httpresponse.ErrConflict.With(err)
	case errors.Is(err, ErrNotImplemented):
		return httpresponse.ErrNotImplemented.With(err)
	case errors.Is(err, ErrServiceUnavailable):
		return httpresponse.ErrServiceUnavailable.With(err)
	default:
		return httpresponse.ErrInternalError.With(err)
	}
}
