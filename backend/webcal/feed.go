package webcal

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	// Packages
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// source is a feed, as its lines: those of the calendar's properties, its
// time zones, of their TZIDs, and its events and tasks, each with the
// components within it, such as its alarms, so an event's file is of its
// lines, as they are, rather than of what's decoded of them
type source struct {
	name, tz   string            // the calendar's name, and zone, of its properties
	header     []line            // the calendar's properties
	zones      map[string][]line // the VTIMEZONEs, of their TZIDs
	components []component       // the VEVENTs and VTODOs, in the feed's order
}

// component is an event, or a task, of its lines, from its BEGIN to its END,
// and what's decoded of it
type component struct {
	lines []line
	event *ical.Event
}

// line is a content line, unfolded, as text, which is its name, of its
// parameters, and its value, and as the lines it was folded as
type line struct {
	name     string   // its name, in upper case, such as DTSTAMP
	text     string   // unfolded
	physical []string // as it was, folded
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// maxFeed is the most octets of a feed
	maxFeed = 64 << 20

	// beginCalendar is how a calendar starts
	beginCalendar = "BEGIN:VCALENDAR"
)

// reTZID is a TZID parameter of a property, quoted, or not
var reTZID = regexp.MustCompile(`(?i);TZID=(?:"([^"]*)"|([^;:]*))`)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// parseFeed returns the source of a feed, which is an iCalendar, of
// BEGIN:VCALENDAR, after a byte order mark, and spaces, whose components
// begin and end in turn, and which isn't cut off, so a page, such as of a
// sign-in, an empty response, or one which ends before the calendar does,
// isn't a calendar of fewer events. An event, or task, which go-metadata
// doesn't decode, such as one without a start, isn't one of its components.
func parseFeed(data []byte) (*source, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	if !bytes.HasPrefix(bytes.ToUpper(bytes.TrimLeft(data, " \t\r\n")), []byte(beginCalendar)) {
		return nil, fmt.Errorf("it isn't an iCalendar")
	}
	self := &source{zones: make(map[string][]line)}
	seen := make(map[string]bool) // the calendar's properties, of which the first is kept
	var stack []string
	var current []line // the lines of a component, of the calendar's
	for _, l := range unfold(data) {
		switch l.name {
		case "BEGIN":
			kind := strings.ToUpper(strings.TrimSpace(value(l.text)))
			stack = append(stack, kind)
			if len(stack) == 1 && kind != "VCALENDAR" {
				return nil, fmt.Errorf("%s isn't in a calendar", kind)
			} else if len(stack) >= 2 {
				current = append(current, l)
			}
		case "END":
			kind := strings.ToUpper(strings.TrimSpace(value(l.text)))
			if len(stack) == 0 || stack[len(stack)-1] != kind {
				return nil, fmt.Errorf("END:%s doesn't end what began", kind)
			}
			stack = stack[:len(stack)-1]
			if len(stack) < 1 {
				continue
			}
			current = append(current, l)
			if len(stack) == 1 {
				self.add(kind, current)
				current = nil
			}
		default:
			switch {
			case len(stack) == 1 && !seen[l.name]:
				seen[l.name] = true
				self.header = append(self.header, l)
			case len(stack) >= 2:
				current = append(current, l)
			}
		}
	}
	if len(stack) > 0 {
		return nil, fmt.Errorf("it ends before %s does", stack[len(stack)-1])
	}

	// The calendar's name, and zone, and what's decoded of each event, of
	// its own calendar
	if calendar, err := ical.Decode(bytes.NewReader(self.calendar(nil, nil))); err == nil {
		self.name, self.tz = strings.TrimSpace(calendar.Name), calendar.TZ
	}
	components := self.components[:0]
	for _, c := range self.components {
		if calendar, err := ical.Decode(bytes.NewReader(self.calendar(nil, c.lines))); err == nil && len(calendar.Events) == 1 {
			c.event = calendar.Events[0]
			components = append(components, c)
		}
	}
	self.components = components
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// add adds a component of the calendar: a time zone, of its TZID, or an
// event, or a task, and not others, such as a journal
func (self *source) add(kind string, lines []line) {
	switch kind {
	case "VTIMEZONE":
		for _, l := range lines {
			if l.name == "TZID" {
				if tzid := strings.TrimSpace(value(l.text)); tzid != "" {
					self.zones[tzid] = lines
				}
				return
			}
		}
	case "VEVENT", "VTODO":
		self.components = append(self.components, component{lines: lines})
	}
}

// calendar returns an iCalendar of the calendar's properties, the time
// zones, and the lines of components, with CRLFs, as they were folded
func (self *source) calendar(zones []string, lines []line) []byte {
	var b bytes.Buffer
	write := func(l line) {
		for _, p := range l.physical {
			b.WriteString(p)
			b.WriteString("\r\n")
		}
	}
	b.WriteString(beginCalendar + "\r\n")
	hasVersion := false
	for _, l := range self.header {
		hasVersion = hasVersion || l.name == "VERSION"
		write(l)
	}
	if !hasVersion {
		b.WriteString("VERSION:2.0\r\n")
	}
	for _, tzid := range zones {
		for _, l := range self.zones[tzid] {
			write(l)
		}
	}
	for _, l := range lines {
		write(l)
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.Bytes()
}

// tzids returns the TZIDs which the lines' properties are of, which are the
// calendar's time zones, in the order they're first of
func (self *source) tzids(lines []line) []string {
	var result []string
	for _, l := range lines {
		for _, match := range reTZID.FindAllStringSubmatch(params(l.text), -1) {
			tzid := strings.TrimSpace(match[1] + match[2])
			if _, exists := self.zones[tzid]; exists && !contains(result, tzid) {
				result = append(result, tzid)
			}
		}
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// unfold returns the content lines of data, unfolded, of the lines they
// were folded as, without those which are empty
func unfold(data []byte) []line {
	var result []line
	for _, p := range strings.Split(string(data), "\n") {
		p = strings.TrimRight(p, "\r")
		switch {
		case p == "":
			continue
		case (p[0] == ' ' || p[0] == '\t') && len(result) > 0:
			last := &result[len(result)-1]
			last.text += p[1:]
			last.physical = append(last.physical, p)
		default:
			result = append(result, line{text: p, physical: []string{p}})
		}
	}
	for i := range result {
		name, _, _ := strings.Cut(params(result[i].text), ";")
		result[i].name = strings.ToUpper(strings.TrimSpace(name))
	}
	return result
}

// params returns a content line's name, and its parameters, which are
// before the first colon which isn't in a quoted parameter
func params(text string) string {
	quoted := false
	for i, c := range text {
		switch {
		case c == '"':
			quoted = !quoted
		case c == ':' && !quoted:
			return text[:i]
		}
	}
	return text
}

// value returns a content line's value, after its name, and its parameters
func value(text string) string {
	return strings.TrimPrefix(text[len(params(text)):], ":")
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
