package rss

import (
	"strconv"
	"strings"
	"time"

	// Packages
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

// Parse a duration string as minutes (for TTL)
func (d *Duration) Minutes() (time.Duration, error) {
	var value string
	if d != nil && d.Value != "" {
		value = strings.TrimSpace(d.Value)
	}
	if value == "" {
		return 0, httpresponse.ErrBadRequest.Withf("duration")
	}
	if minutes, err := strconv.ParseUint(value, 10, 64); err != nil {
		return 0, httpresponse.ErrBadRequest.Withf("%q", value)
	} else {
		return time.Duration(minutes) * time.Minute, nil
	}
}

// Parse a duration string into seconds (for durations)
// This will accept hours:minutes:seconds, minutes:seconds or seconds
func (d *Duration) Seconds() (time.Duration, error) {
	var duration time.Duration
	var value string
	if d != nil && d.Value != "" {
		value = strings.TrimSpace(d.Value)
	}
	if value == "" {
		return 0, httpresponse.ErrBadRequest.Withf("duration")
	}
	hms := strings.SplitN(value, ":", 3)
	switch len(hms) {
	case 1:
		if seconds, err := strconv.ParseUint(hms[0], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(seconds) * time.Second
		}
	case 2:
		if minutes, err := strconv.ParseUint(hms[0], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(minutes) * time.Minute
		}
		if seconds, err := strconv.ParseUint(hms[1], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(seconds) * time.Second
		}
	case 3:
		if hours, err := strconv.ParseUint(hms[0], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(hours) * time.Hour
		}
		if minutes, err := strconv.ParseUint(hms[1], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(minutes) * time.Minute
		}
		if seconds, err := strconv.ParseUint(hms[2], 10, 64); err != nil {
			return 0, httpresponse.ErrBadRequest.Withf("%q", value)
		} else {
			duration += time.Duration(seconds) * time.Second
		}
	}

	// Return success
	return duration, nil
}
