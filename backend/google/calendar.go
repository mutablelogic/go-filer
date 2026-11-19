package google

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CalendarFiler is a filer of the events of a Google account's calendars,
// of the days before today, and after it, of its window, as iCalendars,
// in a directory of each year, and of each month, such as
// /2026/2026-11/20-work@example.com-abc123.ics, which is the day it starts,
// in its own zone, its calendar, and its id. An event which repeats is an
// event of each time it occurs, so an event is of a day, which it starts
// on. It's read, and watched, but not written, so far.
type CalendarFiler struct {
	calendarOpt
	name   string
	opts   gofiler.Opts // Its credentials, and client options, which a clone is created with
	tokens *tokens
	client *httpclient.CalendarClient

	// now returns the time, which the months listed are of
	now func() time.Time
}

// calendarOpt are the options of a calendar filer, of its parameters
type calendarOpt struct {
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How often changes are polled for, or zero not to
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in
	description     string        // What the volume is, which is of the user who signed in, by default
	window          time.Duration // The time of events, before today, and after it, of whole days
}

// calendarPath is a path of a calendar filer: the root, a year, a month, or
// an event of a day of a month, of its calendar's name, and its id
type calendarPath struct {
	year, month, day int
	calendar, event  string
}

// calendarEvent is an event, of its calendar, and its path
type calendarEvent struct {
	calendar *httpclient.Calendar
	event    *httpclient.Event
	path     string
}

var _ gofiler.Filer = (*CalendarFiler)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// CalendarScheme is the scheme of the URL of a volume of a Google
	// account's calendars, gcalendar://<name>
	CalendarScheme = "gcalendar"

	// CalendarNamespace is the namespace of the metadata of events
	CalendarNamespace = "calendar"

	// DefaultCalendarWindow is the time of events, before today, and after
	// it, which are listed, unless the window parameter sets another
	DefaultCalendarWindow = 365 * day

	// paramWindow is the parameter of the time of events which are listed
	paramWindow = "window"

	// maxCalendarWindow is the most time of events which are listed, before
	// today, and after it
	maxCalendarWindow = 3650 * day

	// day is the unit of a window
	day = 24 * time.Hour

	// eventExt is the extension of an event's file
	eventExt = ".ics"

	// dayMargin is the time before and after the months listed which events
	// are got for, as an event's day is in its own zone, which is up to 14
	// hours from UTC
	dayMargin = 24 * time.Hour
)

var (
	// calendarEndpoint is the endpoint of the Calendar API, which is
	// Google's, unless a test sets another
	calendarEndpoint = httpclient.CalendarEndpoint

	// reYear, reMonth and reEventName match the names of a year's
	// directory, a month's, such as 2026-11, and an event's file: its day,
	// its calendar, and its id, which has no "-"
	reYear      = regexp.MustCompile(`^(\d{4})$`)
	reMonth     = regexp.MustCompile(`^(\d{4})-(\d{2})$`)
	reEventName = regexp.MustCompile(`^(\d{2})-(.+)-([A-Za-z0-9_]+)\.ics$`)
	reEventId   = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

	// reCalendarUnsafe matches the characters of a calendar's id which aren't
	// in its name, such as the # of a holiday calendar's
	reCalendarUnsafe = regexp.MustCompile(`[^A-Za-z0-9@._+-]`)

	// eventCategories are the categories of the types of Google's events,
	// other than its default
	eventCategories = map[string]string{
		"birthday":        "Birthday",
		"outOfOffice":     "Out of office",
		"focusTime":       "Focus time",
		"workingLocation": "Working location",
		"fromGmail":       "From Gmail",
	}
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewCalendarFiler returns a filer of the Google Calendar events of the
// volume's name, with the parameters, the user's sign-in, of the provider
// of credentials, and go-client's options of its requests. The sign-in,
// and the calendars, are checked, as for NewTaskFiler. Its parameters are
// readonly, which must be true, so far, quota, refresh_interval and
// credential, as a task filer's, and window, the time of events which are
// listed, before today, and after it, such as 30d, 52w, or 720h, which is
// DefaultCalendarWindow, of whole days.
func NewCalendarFiler(ctx context.Context, name string, params url.Values, opts gofiler.Opts) (*CalendarFiler, error) {
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseCalendarOpts(params)
	if err != nil {
		return nil, err
	}
	opts.ClientOpts = slices.Clone(opts.ClientOpts)
	self := &CalendarFiler{calendarOpt: opt, name: name, opts: opts, now: time.Now}

	// The user's sign-in, which can read calendars
	required := []string{httpclient.CalendarReadonlyScope, httpclient.CalendarScope}
	if self.tokens, err = newTokens(ctx, opts.Credentials, opt.credential, required, []string{httpclient.CalendarReadonlyScope, httpclient.EmailScope}); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	clientopts := append(slices.Clone(opts.ClientOpts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		clientopts = append(clientopts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewCalendarClient(opt.endpoint, clientopts...); err != nil {
		return nil, err
	}

	// Check the calendars can be read
	if err := self.client.Check(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}

	// What the volume is, when it isn't set, is of the user who signed in
	if self.description == "" {
		self.description = describe(ctx, "Google Calendar", self.tokens, opts.ClientOpts)
	}
	return self, nil
}

// parseCalendarOpts returns the options of the parameters, which are
// rejected when they can't be parsed, or are for writing
func parseCalendarOpts(params url.Values) (calendarOpt, error) {
	opt := calendarOpt{quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: calendarEndpoint, window: DefaultCalendarWindow}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential, paramWindow, schema.OptDescription:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
	opt.description = params.Get(schema.OptDescription)
	if value := params.Get(paramReadonly); value != "" {
		if readonly, err := strconv.ParseBool(value); err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramReadonly, value)
		} else if !readonly {
			return opt, gofiler.ErrNotImplemented.With("writing events isn't implemented yet, so a volume of calendars is read-only")
		}
	}
	if value := params.Get(paramQuota); value != "" {
		quota, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramQuota, value)
		}
		opt.quota = uint(quota)
	}
	if value := params.Get(paramRefreshInterval); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < 0 {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramRefreshInterval, value)
		}
		opt.refreshInterval = interval
	}
	if value := params.Get(paramCredential); value != "" {
		if err := credentialKey(value); err != nil {
			return opt, err
		}
		opt.credential = value
	}
	if value := params.Get(paramWindow); value != "" {
		window, err := types.ParseDuration(value)
		if err != nil || window < 0 || window > maxCalendarWindow {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q, rather than a time, such as 30d, 52w or 720h, of up to %s", paramWindow, value, formatWindow(maxCalendarWindow))
		}
		opt.window = window.Truncate(day)
	}
	return opt, nil
}

// Close the filer
func (self *CalendarFiler) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// URL returns the URL of the filer, such as gcalendar://calendar, with
// each of its parameters
func (self *CalendarFiler) URL() *url.URL {
	return &url.URL{Scheme: CalendarScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the calendars, with the parameters changed
// by the patch, and the same client options, which is checked as a new
// filer is
func (self *CalendarFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	// A clone of another sign-in, without a description, is described of it
	params := gofiler.PatchValues(self.params(), patch)
	credential := params.Get(paramCredential)
	if credential == "" {
		credential = DefaultCredential
	}
	if credential != self.credential && !patch.Has(schema.OptDescription) {
		params.Del(schema.OptDescription)
	}
	clone, err := NewCalendarFiler(ctx, self.name, params, self.opts)
	if err != nil {
		return nil, err
	}
	clone.now = self.now
	return clone, nil
}

// Readonly returns true, as events aren't written, so far
func (self *CalendarFiler) Readonly() bool {
	return true
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECTS

// GetObject returns the root, a year, or a month, which are directories,
// or an event
func (self *CalendarFiler) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, _, err := self.stat(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	} else if object.NotModified(req) {
		return nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return object, nil
}

// ReadObject returns the function which reads an event's iCalendar, and the
// event. A directory can't be read, which is fs.ErrInvalid.
func (self *CalendarFiler) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	object, content, err := self.stat(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	} else if object.NotModified(req) {
		return nil, nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(&contentReader{ctx: ctx, r: bytes.NewReader(content)})
	}, object, nil
}

// ListObjects lists the years of the root, the months of a year, or the
// events of a month, or, when it's recursive, every directory and event
// beneath it, or only an event, in the order of their paths, after the
// path the request starts after
func (self *CalendarFiler) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	p, err := self.parsePath("list", req.Path)
	if err != nil {
		return nil, err
	} else if p.event != "" {
		object, _, err := self.stat(ctx, "list", req.Path)
		if err != nil {
			return nil, err
		}
		var objects []*schema.Object
		if object.Path > req.StartAfter {
			objects = append(objects, object)
		}
		return &objectList{objects: objects}, nil
	}

	// The events of the directory, of every month of it which is listed
	from, to := self.span(p)
	events, err := self.events(ctx, "list", req.Path, from, to)
	if err != nil {
		return nil, err
	}
	objects := self.tree(p, events, req.Recursive)
	result := make([]*schema.Object, 0, len(objects))
	for _, object := range objects {
		if sortKey(object) > req.StartAfter {
			result = append(result, object)
		}
	}
	return &objectList{objects: result}, nil
}

// CreateObject isn't implemented, as events aren't written, so far
func (self *CalendarFiler) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

// DeleteObjects isn't implemented, as events aren't written, so far
func (self *CalendarFiler) DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the filer's parameters, so its URL says how it's
// set up
func (self *CalendarFiler) params() url.Values {
	return url.Values{
		paramReadonly:         {"true"},
		paramQuota:            {strconv.FormatUint(uint64(self.quota), 10)},
		paramRefreshInterval:  {self.refreshInterval.String()},
		paramCredential:       {self.credential},
		paramWindow:           {formatWindow(self.window)},
		schema.OptDescription: {self.description},
	}
}

// apiError returns the error of a request, as a task filer's does
func (self *CalendarFiler) apiError(op, path string, err error) error {
	return apiError(self.tokens, self.credential, op, path, err)
}

// days returns the first day which is listed, and the day after the last,
// at midnight, in UTC, which are of the window, before today, and after it
func (self *CalendarFiler) days() (time.Time, time.Time) {
	now := self.now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	return today.Add(-self.window), today.Add(self.window + day)
}

// listed returns true when the month of the year has a day which is listed
func (self *CalendarFiler) listed(year, month int) bool {
	from, to := self.days()
	t := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return t.Before(to) && t.AddDate(0, 1, 0).After(from)
}

// calendarMonths returns the years which are listed, in order, and the months of
// each of them
func (self *CalendarFiler) calendarMonths() ([]int, map[int][]int) {
	from, to := self.days()
	var years []int
	months := make(map[int][]int)
	for t := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC); t.Before(to); t = t.AddDate(0, 1, 0) {
		if len(months[t.Year()]) == 0 {
			years = append(years, t.Year())
		}
		months[t.Year()] = append(months[t.Year()], int(t.Month()))
	}
	return years, months
}

// span returns the days of a path which are listed: of the window, of the
// root, or of a year, or a month, which are within it
func (self *CalendarFiler) span(p calendarPath) (time.Time, time.Time) {
	from, to := self.days()
	switch {
	case p.month != 0:
		month := time.Date(p.year, time.Month(p.month), 1, 0, 0, 0, 0, time.UTC)
		from, to = maxTime(from, month), minTime(to, month.AddDate(0, 1, 0))
	case p.year != 0:
		from = maxTime(from, time.Date(p.year, 1, 1, 0, 0, 0, 0, time.UTC))
		to = minTime(to, time.Date(p.year+1, 1, 1, 0, 0, 0, 0, time.UTC))
	}
	return from, to
}

// parsePath returns the path of the root, a year, /2026, a month,
// /2026/2026-11, or an event, /2026/2026-11/20-<calendar>-<id>.ics, which
// is fs.ErrNotExist of another path, or a year, or month, which isn't
// listed
func (self *CalendarFiler) parsePath(op, p string) (calendarPath, error) {
	notExist := &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	rel := strings.Trim(path.Clean("/"+p), "/")
	if rel == "" {
		return calendarPath{}, nil
	}
	parts := strings.Split(rel, "/")
	var result calendarPath
	if m := reYear.FindStringSubmatch(parts[0]); m == nil {
		return result, notExist
	} else {
		result.year, _ = strconv.Atoi(m[1])
	}
	if len(parts) > 1 {
		m := reMonth.FindStringSubmatch(parts[1])
		if m == nil || m[1] != parts[0] {
			return result, notExist
		}
		result.month, _ = strconv.Atoi(m[2])
		if result.month < 1 || result.month > 12 || !self.listed(result.year, result.month) {
			return result, notExist
		}
	} else {
		// A year of a month which is listed
		from, to := self.days()
		if result.year < from.Year() || result.year > to.AddDate(0, 0, -1).Year() {
			return result, notExist
		}
	}
	if len(parts) > 2 {
		m := reEventName.FindStringSubmatch(parts[2])
		if len(parts) > 3 || strings.HasSuffix(p, "/") || m == nil {
			return result, notExist
		}
		result.day, _ = strconv.Atoi(m[1])
		result.calendar, result.event = m[2], m[3]
	}
	return result, nil
}

// stat returns a directory, or an event, and its iCalendar
func (self *CalendarFiler) stat(ctx context.Context, op, p string) (*schema.Object, []byte, error) {
	path, err := self.parsePath(op, p)
	if err != nil {
		return nil, nil, err
	}
	years, months := self.calendarMonths()
	switch {
	case path.year == 0:
		// The root, of its years
		return dirObject(self.name, "/", len(years), time.Time{}), nil, nil
	case path.month == 0:
		// A year, of its months which are listed
		return dirObject(self.name, pathOfDir(path), len(months[path.year]), time.Time{}), nil, nil
	case path.event == "":
		// A month, of its events, and when they were last changed
		from, to := self.span(path)
		events, err := self.events(ctx, op, p, from, to)
		if err != nil {
			return nil, nil, err
		}
		var modtime time.Time
		for _, e := range events {
			modtime = maxTime(modtime, e.event.Updated)
		}
		return dirObject(self.name, pathOfDir(path), len(events), modtime), nil, nil
	}

	// An event, of its calendar, of its name, which is where it is now
	calendars, err := self.client.Calendars(ctx)
	if err != nil {
		return nil, nil, self.apiError(op, p, err)
	}
	for i := range calendars {
		if calendarName(calendars[i].Id) != path.calendar {
			continue
		}
		event, err := self.client.Event(ctx, calendars[i].Id, path.event)
		if err != nil {
			return nil, nil, self.apiError(op, p, err)
		} else if event.Status == "cancelled" || self.eventPath(&calendars[i], event) != pathOfEvent(p) || !self.inWindow(event) {
			break
		}
		object, content := self.eventObject(&calendars[i], event)
		return object, content, nil
	}
	return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
}

// events returns the events of every calendar which start on one of the
// days, in its own zone, in the order of their paths
func (self *CalendarFiler) events(ctx context.Context, op, p string, from, to time.Time) ([]calendarEvent, error) {
	calendars, err := self.client.Calendars(ctx)
	if err != nil {
		return nil, self.apiError(op, p, err)
	}
	var result []calendarEvent
	for i := range calendars {
		events, err := self.client.Events(ctx, calendars[i].Id, from.Add(-dayMargin), to.Add(dayMargin))
		if err != nil {
			return nil, self.apiError(op, p, err)
		}
		for j := range events {
			if !reEventId.MatchString(events[j].Id) || !onDays(&events[j], from, to) {
				continue
			}
			result = append(result, calendarEvent{calendar: &calendars[i], event: &events[j], path: self.eventPath(&calendars[i], &events[j])})
		}
	}
	slices.SortFunc(result, func(a, b calendarEvent) int { return strings.Compare(a.path, b.path) })
	return result, nil
}

// tree returns the objects of a directory, of its events: its years, of
// the root, its months, of a year, or its events, of a month, or, when
// it's recursive, every directory, and event, beneath it, in the order of
// their keys. Every month which is listed is a directory, as is each of its
// years, whether or not they have events.
func (self *CalendarFiler) tree(p calendarPath, events []calendarEvent, recursive bool) []*schema.Object {
	// The events of each month, and the months of each year, which are listed
	byMonth := make(map[string][]calendarEvent)
	for _, e := range events {
		byMonth[path.Dir(e.path)] = append(byMonth[path.Dir(e.path)], e)
	}
	years, months := self.calendarMonths()
	var result []*schema.Object
	add := func(object *schema.Object, depth int) {
		if depth == 1 || recursive {
			result = append(result, object)
		}
	}
	for _, year := range years {
		yearPath := fmt.Sprintf("/%04d", year)
		depth := 1
		switch {
		case p.year == 0:
		case p.year != year:
			continue
		default:
			depth = 0
		}
		if depth == 1 {
			add(dirObject(self.name, yearPath, len(months[year]), time.Time{}), 1)
		}
		for _, month := range months[year] {
			monthPath := fmt.Sprintf("%s/%04d-%02d", yearPath, year, month)
			monthDepth := depth + 1
			switch {
			case p.month == 0:
			case p.month != month:
				continue
			default:
				monthDepth = 0
			}
			if monthDepth > 0 {
				var modtime time.Time
				for _, e := range byMonth[monthPath] {
					modtime = maxTime(modtime, e.event.Updated)
				}
				add(dirObject(self.name, monthPath, len(byMonth[monthPath]), modtime), monthDepth)
			}
			for _, e := range byMonth[monthPath] {
				object, _ := self.eventObject(e.calendar, e.event)
				add(object, monthDepth+1)
			}
		}
	}
	slices.SortFunc(result, func(a, b *schema.Object) int { return strings.Compare(sortKey(a), sortKey(b)) })
	return result
}

// inWindow returns true when an event starts on a day which is listed
func (self *CalendarFiler) inWindow(event *httpclient.Event) bool {
	from, to := self.days()
	return onDays(event, from, to)
}

// eventPath returns the path of an event: of its year, its month, and its
// day, which it starts on, in its own zone, its calendar, and its id
func (self *CalendarFiler) eventPath(calendar *httpclient.Calendar, event *httpclient.Event) string {
	year, month, day, _ := eventDay(event)
	return fmt.Sprintf("/%04d/%04d-%02d/%02d-%s-%s%s", year, year, month, day, calendarName(calendar.Id), event.Id, eventExt)
}

// eventObject returns an event, which is a file, of its iCalendar, with its
// metadata, and its content
func (self *CalendarFiler) eventObject(calendar *httpclient.Calendar, event *httpclient.Event) (*schema.Object, []byte) {
	cal := eventCalendar(calendar, event)
	content, _ := cal.MarshalText()
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: self.eventPath(calendar, event)},
		ObjectMeta: schema.ObjectMeta{ContentType: cal.ContentType()},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(len(content))), ModTime: event.Updated},
	}
	if event.ETag != "" {
		object.ETag = types.Ptr(event.ETag)
	}
	addMeta := func(ns, key, value string) {
		if value != "" {
			object.Meta = append(object.Meta, schema.Meta{Namespace: ns, Key: key, Value: json.RawMessage(quote(value))})
		}
	}
	addMeta(DCNamespace, "title", strings.Join(strings.Fields(event.Summary), " "))
	addMeta(DCNamespace, "identifier", firstOf(event.ICalUID, event.Id))
	addMeta(CalendarNamespace, "url", event.HTMLLink)
	addMeta(CalendarNamespace, "calendar", calendar.Id)
	addMeta(CalendarNamespace, "name", calendarTitle(calendar))
	addMeta(CalendarNamespace, "start", firstOf(event.Start.Date, event.Start.DateTime))
	addMeta(CalendarNamespace, "end", firstOf(event.End.Date, event.End.DateTime))
	addMeta(CalendarNamespace, "status", event.Status)
	addMeta(CalendarNamespace, "series", event.RecurringEventId)
	return object, content
}

////////////////////////////////////////////////////////////////////////////////
// ICALENDAR

// eventCalendar returns an event as an iCalendar, of its VEVENT, of its
// calendar's name, and zone: its UID, which is Google's iCalendar UID, as
// an invitation's is, or its id, and, of a time an event which repeats
// occurs, its RECURRENCE-ID, of when it was to start, its title, description,
// location, link, start and end, which are dates, or times in their zones,
// status, organiser, attendees, its type, as a category, whether it's busy,
// and when it was created, and updated
func eventCalendar(calendar *httpclient.Calendar, event *httpclient.Event) *ical.Calendar {
	vevent := &ical.Event{
		Kind:         ical.KindEvent,
		UID:          firstOf(event.ICalUID, event.Id),
		RecurrenceID: eventTime(event.OriginalStartTime),
		Summary:      strings.Join(strings.Fields(event.Summary), " "),
		Description:  strings.TrimSpace(event.Description),
		Location:     strings.TrimSpace(event.Location),
		URL:          event.HTMLLink,
		Start:        eventTime(event.Start),
		End:          eventTime(event.End),
		Status:       strings.ToUpper(event.Status),
		Organizer:    event.Organizer.Email,
		Transparent:  event.Transparency == "transparent",
		Stamp:        event.Updated,
		Created:      event.Created,
		Modified:     event.Updated,
	}
	for _, attendee := range event.Attendees {
		if attendee.Email != "" {
			vevent.Attendees = append(vevent.Attendees, attendee.Email)
		}
	}
	if category, exists := eventCategories[event.EventType]; exists {
		vevent.Categories = append(vevent.Categories, category)
	}
	return &ical.Calendar{ProdID: ProdID, Name: calendarTitle(calendar), TZ: calendar.TimeZone, Events: []*ical.Event{vevent}}
}

// eventTime returns when an event starts or ends: a date, of an all-day
// event, or a time, in its zone, when it's known, or in UTC, or none, when
// it can't be parsed
func eventTime(t httpclient.EventTime) ical.Time {
	if t.Date != "" {
		if d, err := time.Parse(time.DateOnly, t.Date); err == nil {
			return ical.Date(d.Year(), d.Month(), d.Day())
		}
		return ical.Time{}
	}
	when, err := time.Parse(time.RFC3339, t.DateTime)
	if err != nil {
		return ical.Time{}
	} else if t.TimeZone != "" {
		if location, err := time.LoadLocation(t.TimeZone); err == nil {
			return ical.In(when, location)
		}
	}
	return ical.UTC(when)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// eventDay returns the day an event starts on, in its own zone, of its
// date, or its time, in its zone, when it's known, as the time's offset can
// be of another, such as its calendar's, or else of its offset, or false
// when it can't be parsed
func eventDay(event *httpclient.Event) (int, int, int, bool) {
	if event.Start.Date != "" {
		d, err := time.Parse(time.DateOnly, event.Start.Date)
		if err != nil {
			return 0, 0, 0, false
		}
		return d.Year(), int(d.Month()), d.Day(), true
	}
	t, err := time.Parse(time.RFC3339, event.Start.DateTime)
	if err != nil {
		return 0, 0, 0, false
	} else if event.Start.TimeZone != "" {
		if location, err := time.LoadLocation(event.Start.TimeZone); err == nil {
			t = t.In(location)
		}
	}
	return t.Year(), int(t.Month()), t.Day(), true
}

// formatWindow returns the time of a window, as days, such as 365d
func formatWindow(window time.Duration) string {
	return strconv.FormatInt(int64(window/day), 10) + "d"
}

// onDays returns true when an event starts on one of the days, in its own
// zone, which are from the first, at midnight, in UTC, to the last
func onDays(event *httpclient.Event, from, to time.Time) bool {
	year, month, d, ok := eventDay(event)
	if !ok {
		return false
	}
	t := time.Date(year, time.Month(month), d, 0, 0, 0, 0, time.UTC)
	return !t.Before(from) && t.Before(to)
}

// calendarName returns the name of a calendar in its events' paths, of its
// id, whose characters which aren't safe, such as #, are _
func calendarName(id string) string {
	return reCalendarUnsafe.ReplaceAllString(id, "_")
}

// calendarTitle returns the name of a calendar, which is the user's, or
// its own
func calendarTitle(calendar *httpclient.Calendar) string {
	return firstOf(calendar.SummaryOverride, calendar.Summary)
}

// pathOfDir returns the path of the root, a year, or a month
func pathOfDir(p calendarPath) string {
	switch {
	case p.month != 0:
		return fmt.Sprintf("/%04d/%04d-%02d", p.year, p.year, p.month)
	case p.year != 0:
		return fmt.Sprintf("/%04d", p.year)
	default:
		return "/"
	}
}

// pathOfEvent returns the clean path of an event's
func pathOfEvent(p string) string {
	return path.Clean("/" + p)
}

// dirObject returns a directory, of how many objects are in it, and when
// what's in it was last changed
func dirObject(name, p string, count int, modtime time.Time) *schema.Object {
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: name, Path: p},
		ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType},
		ObjectAttr: schema.ObjectAttr{IsDir: true, ModTime: modtime},
	}
	object.Meta = append(object.Meta, schema.FolderMeta(count)...)
	return object
}

// firstOf returns the first value which isn't empty
func firstOf(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
