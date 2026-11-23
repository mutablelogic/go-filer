package webcal

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	// Packages
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// source is a feed, which is read once, in turn, from the volume's cache,
// as what's kept of it: the calendar's properties, and its time zones, which
// are small, and of its events and tasks, where they are in it, so their
// lines are read from it, rather than kept
type source struct {
	name, tz   string            // the calendar's name, and zone, of its properties
	header     []byte            // the calendar's properties, as they are
	version    bool              // the calendar's properties have its VERSION
	zones      map[string][]byte // the VTIMEZONEs, of their TZIDs, as they are
	components []component       // the VEVENTs and VTODOs, in the feed's order
}

// component is an event, or a task, of where its lines are in the feed, and
// what it is, once they're rendered: when it's stamped, the time zones it's
// in, and the hash, and length, of its lines
type component struct {
	part
	event  *ical.Event // what's decoded of it, for its path, and metadata
	tzids  []string    // the time zones its times are in, which are the feed's
	sum    [32]byte    // the hash of its lines, without its DTSTAMP
	length int         // how many octets its lines are, rendered
}

// part is where a component's lines are in the feed, and the DTSTAMP it's
// rendered with, or zero, of its own
type part struct {
	off, n int64
	stamp  time.Time
}

// line is a content line, unfolded, as text, of the lines it was folded as
type line struct {
	name     string   // its name, in upper case, such as DTSTAMP
	text     string   // unfolded
	physical []string // as it was, folded, without line breaks
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// maxFeed is the most octets of a feed
	maxFeed = 64 << 20

	// beginCalendar, and endCalendar, are how a calendar starts, and ends
	beginCalendar = "BEGIN:VCALENDAR\r\n"
	endCalendar   = "END:VCALENDAR\r\n"
	version       = "VERSION:2.0\r\n"

	// stampLayout is the layout of a DTSTAMP, in UTC
	stampLayout = "20060102T150405Z"
)

var (
	// reTZID is a TZID parameter of a property, quoted, or not
	reTZID = regexp.MustCompile(`(?i);TZID=(?:"([^"]*)"|([^;:]*))`)

	// errNotCalendar is a feed which isn't an iCalendar, such as a page
	errNotCalendar = errors.New("it isn't an iCalendar")
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// parseFeed reads a feed, in turn, which is an iCalendar, of
// BEGIN:VCALENDAR, after a byte order mark, and spaces, whose components
// begin and end in turn, and which isn't cut off, so a page, such as of a
// sign-in, an empty response, or one which ends before the calendar does,
// isn't a calendar of fewer events. What's kept of it is small: its lines
// are read again, when they're rendered, from where they are. An event, or
// task, which go-metadata doesn't decode, such as one without a start,
// isn't one of its components.
func parseFeed(r io.Reader) (*source, error) {
	self := &source{zones: make(map[string][]byte)}
	reader := bufio.NewReaderSize(r, 64<<10)
	seen := make(map[string]bool) // the calendar's properties, of which the first is kept
	var stack []string
	var raw []byte // the lines of a component, as they are
	var start int64
	var off int64
	first := true

	// Each content line, unfolded, of where it starts, and where the line
	// after it starts
	var pending *line
	var pendingStart int64
	var pendingRaw []byte
	handle := func(l *line, lstart, lend int64, lraw []byte) error {
		// Its name is of the line, once it's unfolded, as a name can be
		// folded too
		l.name = lineName(l.text)
		switch l.name {
		case "BEGIN":
			kind := strings.ToUpper(strings.TrimSpace(value(l.text)))
			if first && kind != "VCALENDAR" {
				return errNotCalendar
			}
			first = false
			stack = append(stack, kind)
			if len(stack) == 1 && kind != "VCALENDAR" {
				return fmt.Errorf("%s isn't in a calendar", kind)
			} else if len(stack) == 2 {
				raw, start = append(raw[:0], lraw...), lstart
			} else if len(stack) > 2 {
				raw = append(raw, lraw...)
			}
		case "END":
			if first {
				return errNotCalendar
			}
			kind := strings.ToUpper(strings.TrimSpace(value(l.text)))
			if len(stack) == 0 || stack[len(stack)-1] != kind {
				return fmt.Errorf("END:%s doesn't end what began", kind)
			}
			stack = stack[:len(stack)-1]
			if len(stack) >= 1 {
				raw = append(raw, lraw...)
			}
			if len(stack) == 1 {
				self.add(kind, raw, start, lend)
			}
		default:
			if first {
				return errNotCalendar
			}
			switch {
			case len(stack) == 1 && !seen[l.name]:
				seen[l.name] = true
				self.header = append(self.header, render(l)...)
			case len(stack) >= 2:
				raw = append(raw, lraw...)
			}
		}
		return nil
	}

	for {
		physical, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		lstart := off
		off += int64(len(physical))
		if off > maxFeed {
			return nil, fmt.Errorf("it's more than %d MiB", maxFeed>>20)
		}
		text := strings.TrimRight(physical, "\r\n")
		if lstart == 0 {
			text = strings.TrimPrefix(text, "\xef\xbb\xbf")
		}
		switch {
		case text == "" && err == nil:
			// An empty line, which is part of a component it's in
			if len(stack) >= 2 && pending == nil {
				raw = append(raw, physical...)
			} else if pending != nil {
				pendingRaw = append(pendingRaw, physical...)
			}
			continue
		case text != "" && (text[0] == ' ' || text[0] == '\t') && pending != nil:
			pending.text += text[1:]
			pending.physical = append(pending.physical, text)
			pendingRaw = append(pendingRaw, physical...)
		case text != "":
			if pending != nil {
				if err := handle(pending, pendingStart, lstart, pendingRaw); err != nil {
					return nil, err
				}
			}
			pending, pendingStart, pendingRaw = &line{text: text, physical: []string{text}}, lstart, append([]byte(nil), physical...)
		}
		if err == io.EOF {
			break
		}
	}
	if pending != nil {
		if err := handle(pending, pendingStart, off, pendingRaw); err != nil {
			return nil, err
		}
	}
	if first {
		return nil, errNotCalendar
	} else if len(stack) > 0 {
		return nil, fmt.Errorf("it ends before %s does", stack[len(stack)-1])
	}

	// The calendar's name, and zone
	self.version = seen["VERSION"]
	if calendar, err := ical.Decode(bytes.NewReader(self.calendar(nil, nil))); err == nil {
		self.name, self.tz = strings.TrimSpace(calendar.Name), calendar.TZ
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// add adds a component of the calendar, of its lines, and where they are: a
// time zone, of its TZID, or an event, or a task, which is decoded, and
// rendered, so what it is, once it is, is known, and not others, such as a
// journal
func (self *source) add(kind string, raw []byte, start, end int64) {
	lines := unfold(raw)
	switch kind {
	case "VTIMEZONE":
		for _, l := range lines {
			if l.name == "TZID" {
				if tzid := strings.TrimSpace(value(l.text)); tzid != "" {
					var b []byte
					for _, l := range lines {
						b = append(b, render(&l)...)
					}
					self.zones[tzid] = b
				}
				return
			}
		}
	case "VEVENT", "VTODO":
		calendar, err := ical.Decode(bytes.NewReader(self.calendar(nil, [][]byte{raw})))
		if err != nil || len(calendar.Events) != 1 {
			return
		}
		e := calendar.Events[0]
		c := component{part: part{off: start, n: end - start, stamp: e.Modified}, event: e}
		if c.stamp.IsZero() {
			c.stamp = e.Created
		}
		out, hashed := renderComponent(raw, c.stamp)
		c.sum, c.length = sha256.Sum256(hashed), len(out)
		for _, l := range lines {
			for _, match := range reTZID.FindAllStringSubmatch(params(l.text), -1) {
				if tzid := strings.TrimSpace(match[1] + match[2]); !slices.Contains(c.tzids, tzid) {
					c.tzids = append(c.tzids, tzid)
				}
			}
		}
		self.components = append(self.components, c)
	}
}

// calendar returns an iCalendar of the calendar's properties, the time
// zones, and components, as they are
func (self *source) calendar(zones []string, components [][]byte) []byte {
	var b bytes.Buffer
	b.Write(self.prefix(zones))
	for _, c := range components {
		b.Write(c)
	}
	b.WriteString(endCalendar)
	return b.Bytes()
}

// prefix returns the start of an iCalendar, of the calendar's properties,
// and the time zones, of those which are the feed's
func (self *source) prefix(zones []string) []byte {
	var b bytes.Buffer
	b.WriteString(beginCalendar)
	b.Write(self.header)
	if !self.version {
		b.WriteString(version)
	}
	for _, tzid := range zones {
		b.Write(self.zones[tzid])
	}
	return b.Bytes()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// renderComponent returns a component's lines, with CRLFs, as they were
// folded, and its DTSTAMP, when it's stamped, and its lines without its
// DTSTAMP, which its hash is of
func renderComponent(raw []byte, stamp time.Time) ([]byte, []byte) {
	var out, hashed []byte
	for _, l := range unfold(raw) {
		switch {
		case l.name != "DTSTAMP":
			r := render(&l)
			out, hashed = append(out, r...), append(hashed, r...)
		case stamp.IsZero():
			out = append(out, render(&l)...)
		default:
			out = append(out, "DTSTAMP:"+stamp.UTC().Format(stampLayout)+"\r\n"...)
		}
	}
	return out, hashed
}

// render returns a line, as it was folded, with CRLFs
func render(l *line) []byte {
	var b []byte
	for _, p := range l.physical {
		b = append(b, p...)
		b = append(b, '\r', '\n')
	}
	return b
}

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

	// Their names are of the lines, once they're unfolded, as a name can be
	// folded too
	for i := range result {
		result[i].name = lineName(result[i].text)
	}
	return result
}

// lineName returns a content line's name, in upper case, such as DTSTAMP,
// of the line, unfolded
func lineName(text string) string {
	name, _, _ := strings.Cut(params(text), ";")
	return strings.ToUpper(strings.TrimSpace(name))
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
