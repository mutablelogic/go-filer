package ics

import (
	"bufio"
	"context"
	"io"
	"regexp"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type icsextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(icsextractor))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *icsextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`^text/calendar$`)
}

func (e *icsextractor) ExtractMetadata(_ context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	props := parse(r)

	var (
		kv         []schema.Meta
		eventCount int
		summaries  []string
		start      time.Time
		end        time.Time
		organizers []string
		locations  []string
		categories   []string
		descriptions []string
		urls         []string
		recurrences  []string
	)

	for _, p := range props {
		inEvent := p.component == "VEVENT"
		switch p.name {
		case "SUMMARY":
			if inEvent && p.value != "" {
				summaries = append(summaries, p.value)
			}
		case "DTSTART":
			if inEvent {
				if t, err := parseDate(p.value); err == nil {
					eventCount++
					if start.IsZero() || t.Before(start) {
						start = t
					}
				}
			}
		case "DTEND":
			if inEvent {
				if t, err := parseDate(p.value); err == nil {
					if end.IsZero() || t.After(end) {
						end = t
					}
				}
			}
		case "ORGANIZER":
			if inEvent {
				v := strings.TrimPrefix(p.value, "mailto:")
				if v != "" {
					organizers = appendUnique(organizers, v)
				}
			}
		case "LOCATION":
			if inEvent && p.value != "" {
				locations = appendUnique(locations, p.value)
			}
		case "CATEGORIES":
			if inEvent {
				for _, c := range strings.Split(p.value, ",") {
					if c = strings.TrimSpace(c); c != "" {
						categories = appendUnique(categories, c)
					}
				}
			}
		case "DESCRIPTION":
			if inEvent && p.value != "" {
				descriptions = appendUnique(descriptions, p.value)
			}
		case "URL":
			if inEvent && p.value != "" {
				urls = appendUnique(urls, p.value)
			}
		case "RRULE":
			if inEvent {
				freq, until := parseRRule(p.value)
				if freq != "" {
					recurrences = appendUnique(recurrences, freq)
				}
				if !until.IsZero() && (end.IsZero() || until.After(end)) {
					end = until
				}
			}
		}
	}

	kv = schema.AppendMeta(kv, metadata.CalendarEventCount, eventCount)
	if len(summaries) > 0 {
		kv = schema.AppendMeta(kv, metadata.CalendarTitle, summaries[0])
	}
	if !start.IsZero() {
		kv = schema.AppendMeta(kv, metadata.CalendarStart, start.Format(time.RFC3339))
	}
	if !end.IsZero() {
		kv = schema.AppendMeta(kv, metadata.CalendarEnd, end.Format(time.RFC3339))
	}
	if !start.IsZero() && !end.IsZero() && end.After(start) {
		d := end.Sub(start)
		kv = schema.AppendMeta(kv, metadata.CalendarDurationSecs, d.Seconds())
		kv = schema.AppendMeta(kv, metadata.CalendarDuration, d.Truncate(time.Second).String())
	}
	if len(organizers) == 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarOrganizer, organizers[0])
	} else if len(organizers) > 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarOrganizer, organizers)
	}
	if len(locations) == 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarLocation, locations[0])
	} else if len(locations) > 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarLocation, locations)
	}
	if len(categories) == 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarCategories, categories[0])
	} else if len(categories) > 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarCategories, categories)
	}
	if len(recurrences) == 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarRecurrence, recurrences[0])
	} else if len(recurrences) > 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarRecurrence, recurrences)
	}
	if len(descriptions) > 0 {
		kv = schema.AppendMeta(kv, metadata.CalendarDescription, descriptions[0])
	}
	if len(urls) == 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarURL, urls[0])
	} else if len(urls) > 1 {
		kv = schema.AppendMeta(kv, metadata.CalendarURL, urls)
	}

	return kv, nil, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

type prop struct {
	name      string
	value     string
	component string // e.g. VEVENT, VTIMEZONE, VALARM
}

// parse reads iCalendar lines, unfolds them, and returns name/value pairs
// annotated with the component they belong to. Parameters on property names
// (e.g. DTSTART;TZID=...) are stripped.
func parse(r io.Reader) []prop {
	scanner := bufio.NewScanner(r)

	var lines []string
	var current string
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			current += strings.TrimPrefix(strings.TrimPrefix(line, " "), "\t")
		} else {
			if current != "" {
				lines = append(lines, current)
			}
			current = line
		}
	}
	if current != "" {
		lines = append(lines, current)
	}

	var props []prop
	var stack []string // component stack, e.g. ["VCALENDAR", "VEVENT"]
	for _, line := range lines {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		name, _, _ = strings.Cut(name, ";")
		name = strings.ToUpper(strings.TrimSpace(name))
		value = strings.TrimSpace(value)

		switch name {
		case "BEGIN":
			stack = append(stack, strings.ToUpper(value))
		case "END":
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		default:
			component := ""
			if len(stack) > 0 {
				component = stack[len(stack)-1]
			}
			props = append(props, prop{name: name, value: value, component: component})
		}
	}
	return props
}

// parseRRule extracts FREQ and UNTIL from an RRULE value string.
func parseRRule(s string) (freq string, until time.Time) {
	for _, part := range strings.Split(s, ";") {
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch strings.ToUpper(key) {
		case "FREQ":
			freq = strings.ToUpper(val)
		case "UNTIL":
			if t, err := parseDate(val); err == nil {
				until = t
			}
		}
	}
	return
}

// parseDate handles the common iCalendar date/datetime formats.
func parseDate(s string) (time.Time, error) {
	formats := []string{
		"20060102T150405Z",
		"20060102T150405",
		"20060102",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, &time.ParseError{}
}

func appendUnique(ss []string, s string) []string {
	for _, v := range ss {
		if v == s {
			return ss
		}
	}
	return append(ss, s)
}
