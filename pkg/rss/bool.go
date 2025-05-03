package rss

import (
	"strconv"
	"strings"
)

// Parse "complete" or "block" boolean values
func ParseBool(v string) bool {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "y" || v == "yes" {
		return true
	} else if v_, err := strconv.ParseBool(v); err == nil {
		return v_
	} else {
		return false
	}
}
