package rss

import "time"

// Parse a date string
func (d *Date) Parse() (time.Time, error) {
	if t, err := time.Parse(time.RFC1123, d.Value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC1123Z, d.Value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, d.Value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC822, d.Value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC822Z, d.Value); err == nil {
		return t, nil
	}
	return time.Parse("Mon, 2 Jan 2006 15:04:05 MST", d.Value)
}
