package google

import (
	"context"
	"encoding/json"
	"io/fs"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// eventAction creates, updates or deletes an event
type eventAction struct {
	*CalendarFiler
}

// eventInput is the input of the event action. Its fields which are nil
// aren't changed, of an update, and an empty one is cleared.
type eventInput struct {
	Op          string    `json:"op" enum:"create,update,delete" help:"What's done: create an event, update one, or one time one which repeats occurs, or delete one"`
	Path        string    `json:"path,omitempty" help:"Path of the event to update or delete, such as /2026/2026-11/20-<calendar>-<id>.ics"`
	Calendar    string    `json:"calendar,omitempty" help:"Id of the calendar to create the event in, or the primary calendar without it"`
	Title       *string   `json:"title,omitempty" help:"Title of the event, which an event which is created has"`
	Description *string   `json:"description,omitempty" help:"Description of the event"`
	Location    *string   `json:"location,omitempty" help:"Where the event is"`
	Start       *string   `json:"start,omitempty" help:"When it starts: a day, YYYY-MM-DD, of an event of whole days, or a time, of RFC 3339, or YYYY-MM-DDTHH:MM in its time zone"`
	End         *string   `json:"end,omitempty" help:"When it ends, after its start, as its start is: the day after the last, of whole days. Defaults to a day, or an hour, after its start, of an event which is created"`
	TimeZone    *string   `json:"timezone,omitempty" help:"Time zone of its times, such as Europe/Berlin, or its calendar's without it"`
	Attendees   *[]string `json:"attendees,omitempty" help:"Email addresses of those who are invited"`
}

// when is a time an event starts, or ends: a day, at midnight, in UTC, of
// an event of whole days, or a time, of its zone, when it has one
type when struct {
	date bool
	t    time.Time
	zone string
}

var _ gofiler.Action = eventAction{}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns what the volume can do, beyond its objects, when it's
// writable: event, which creates, updates or deletes an event
func (self *CalendarFiler) Actions() []gofiler.Action {
	if self.readonly {
		return nil
	}
	return []gofiler.Action{eventAction{self}}
}

func (eventAction) Name() string {
	return "event"
}

func (eventAction) Title() string {
	return "Create, update or delete an event"
}

func (eventAction) Description() string {
	return "Creates an event in a calendar, of its title and start, or, of the path of an event, such as /2026/2026-11/20-<calendar>-<id>.ics, updates the fields which are given, or deletes it. The path of one time an event which repeats occurs changes that time alone. Returns the event."
}

func (eventAction) Input() *jsonschema.Schema {
	return jsonschema.MustFor[eventInput]()
}

// Run creates an event, or updates, or deletes, the event of the path, and
// returns it, as it is, or was, before it's deleted. What's got from Google
// is got again, so the change is listed, and the watches are sent it, as
// it's made (see wrote). An event, or its path, can be outside the days
// which are listed, though it isn't listed. An event which repeats, rather
// than a time it occurs, isn't changed, as it isn't listed.
func (self eventAction) Run(ctx context.Context, data json.RawMessage) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	var input eventInput
	if err := decodeInput("event", data, &input); err != nil {
		return nil, err
	} else if err := input.check(); err != nil {
		return nil, err
	}
	calendars, err := self.calendars(ctx)
	if err != nil {
		return nil, self.apiError("event", "/", err)
	}
	// What's got from Google is got again, when it fails, as what it
	// changed isn't known
	defer self.clearCache()

	// An event which is created is of a calendar, of its id, or the primary
	if input.Op == opCreate {
		var calendar *httpclient.Calendar
		for i := range calendars {
			if (input.Calendar == "" && calendars[i].Primary) || calendars[i].Id == input.Calendar {
				calendar = &calendars[i]
				break
			}
		}
		if calendar == nil {
			return nil, gofiler.ErrBadParameter.Withf("event: there's no calendar %q", firstOf(input.Calendar, "primary"))
		}
		fields, err := input.fields(calendar, nil)
		if err != nil {
			return nil, err
		}
		event, err := self.client.InsertEvent(ctx, calendar.Id, fields)
		if err != nil {
			return nil, self.apiError("event", "/", err)
		}
		self.wrote(calendar, nil, event)
		object, _ := self.eventObject(calendar, event)
		return object, nil
	}

	// An event which is updated, or deleted, is got of its calendar, of its
	// path, which must be where it is
	path, err := self.parseEventPath("event", input.Path)
	if err != nil {
		return nil, err
	} else if path.event == "" {
		return nil, gofiler.ErrBadParameter.Withf("event: %s's path is of an event, such as /2026/2026-11/20-<calendar>-<id>.ics", input.Op)
	}
	var calendar *httpclient.Calendar
	for i := range calendars {
		if calendarName(calendars[i].Id) == path.calendar {
			calendar = &calendars[i]
			break
		}
	}
	if calendar == nil {
		return nil, &fs.PathError{Op: "event", Path: input.Path, Err: fs.ErrNotExist}
	}
	current, err := self.client.Event(ctx, calendar.Id, path.event)
	if err != nil {
		return nil, self.apiError("event", input.Path, err)
	} else if current.Status == "cancelled" || len(current.Recurrence) > 0 || self.eventPath(calendar, current) != pathOfEvent(input.Path) {
		return nil, &fs.PathError{Op: "event", Path: input.Path, Err: fs.ErrNotExist}
	}
	if input.Op == opDelete {
		object, _ := self.eventObject(calendar, current)
		if err := self.client.DeleteEvent(ctx, calendar.Id, current.Id); err != nil {
			return nil, self.apiError("event", input.Path, err)
		}
		self.wrote(calendar, current, nil)
		return object, nil
	}
	fields, err := input.fields(calendar, current)
	if err != nil {
		return nil, err
	}
	event, err := self.client.PatchEvent(ctx, calendar.Id, current.Id, fields)
	if err != nil {
		return nil, self.apiError("event", input.Path, err)
	}
	self.wrote(calendar, current, event)
	object, _ := self.eventObject(calendar, event)
	return object, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// check returns ErrBadParameter when the input isn't valid for its op
func (input eventInput) check() error {
	fields := input.Title != nil || input.Description != nil || input.Location != nil || input.Start != nil || input.End != nil || input.TimeZone != nil || input.Attendees != nil
	switch {
	case input.Op != opCreate && input.Op != opUpdate && input.Op != opDelete:
		return gofiler.ErrBadParameter.Withf("event: op is create, update or delete, not %q", input.Op)
	case input.Op == opCreate && input.Path != "":
		return gofiler.ErrBadParameter.With("event: create has no path, but a calendar, as Google names the event")
	case input.Op != opCreate && input.Path == "":
		return gofiler.ErrBadParameter.Withf("event: %s needs the path of an event", input.Op)
	case input.Op != opCreate && input.Calendar != "":
		return gofiler.ErrBadParameter.Withf("event: %s's calendar is of its path", input.Op)
	case input.Op == opDelete && fields:
		return gofiler.ErrBadParameter.With("event: delete has only a path")
	case input.Op == opCreate && (input.Title == nil || strings.TrimSpace(*input.Title) == ""):
		return gofiler.ErrBadParameter.With("event: create needs a title")
	case input.Op == opCreate && input.Start == nil:
		return gofiler.ErrBadParameter.With("event: create needs a start")
	case input.Op == opUpdate && !fields:
		return gofiler.ErrBadParameter.With("event: update needs a field to change")
	case input.TimeZone != nil && input.Start == nil && input.End == nil:
		return gofiler.ErrBadParameter.With("event: a time zone is of a start, or an end")
	}
	return nil
}

// fields returns the fields of the input which are written, of an event
// which is created, or of the current event, which is updated, whose length
// is kept when only its start changes. Of an update, its start is written
// when it's given, and its end when it's given, or its start is, so a
// change of one doesn't undo another's of the other, and a day, or a time,
// clears the other, as a patch keeps what isn't written.
func (input eventInput) fields(calendar *httpclient.Calendar, current *httpclient.Event) (httpclient.EventFields, error) {
	fields := httpclient.EventFields{}
	if input.Title != nil {
		title := strings.Join(strings.Fields(*input.Title), " ")
		if title == "" {
			return nil, gofiler.ErrBadParameter.With("event: a title isn't empty")
		}
		fields["summary"] = title
	}
	if input.Description != nil {
		fields["description"] = strings.TrimSpace(*input.Description)
	}
	if input.Location != nil {
		fields["location"] = strings.TrimSpace(*input.Location)
	}
	if input.Attendees != nil {
		attendees := []map[string]string{}
		for _, email := range *input.Attendees {
			if email = strings.TrimSpace(email); email != "" {
				attendees = append(attendees, map[string]string{"email": email})
			}
		}
		fields["attendees"] = attendees
	}

	// Its start, and end, of their zone, or the calendar's, which are both
	// days, or both times, and its end is after its start
	if input.Start == nil && input.End == nil {
		return fields, nil
	}
	zone := calendar.TimeZone
	if input.TimeZone != nil {
		if _, err := time.LoadLocation(*input.TimeZone); err != nil || *input.TimeZone == "" {
			return nil, gofiler.ErrBadParameter.Withf("event: %q isn't a time zone", *input.TimeZone)
		}
		zone = *input.TimeZone
	}
	var start, end when
	var err error
	if input.Start != nil {
		if start, err = parseWhen("start", *input.Start, zone); err != nil {
			return nil, err
		}
	} else if start, err = currentWhen(current.Start); err != nil {
		return nil, err
	}
	switch {
	case input.End != nil:
		if end, err = parseWhen("end", *input.End, zone); err != nil {
			return nil, err
		}
	case current != nil:
		// Its start changed, so it ends as long after it as it did
		from, err := currentWhen(current.Start)
		if err != nil {
			return nil, err
		}
		to, err := currentWhen(current.End)
		if err != nil {
			return nil, err
		} else if from.date != start.date {
			return nil, gofiler.ErrBadParameter.With("event: a start of a day, rather than a time, or of a time, rather than a day, needs an end")
		}
		end = when{date: start.date, t: start.t.Add(to.t.Sub(from.t)), zone: start.zone}
	case start.date:
		end = when{date: true, t: start.t.AddDate(0, 0, 1)}
	default:
		end = when{t: start.t.Add(time.Hour), zone: start.zone}
	}
	if start.date != end.date {
		return nil, gofiler.ErrBadParameter.With("event: its start and end are both days, or both times")
	} else if !end.t.After(start.t) {
		return nil, gofiler.ErrBadParameter.With("event: it ends after it starts")
	}
	clear := current != nil
	if input.Start != nil || current == nil {
		fields["start"] = start.field(clear)
	}
	fields["end"] = end.field(clear)
	return fields, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// parseWhen returns a day, YYYY-MM-DD, or a time, of RFC 3339, or of
// YYYY-MM-DDTHH:MM, or with seconds, in the zone
func parseWhen(name, value, zone string) (when, error) {
	value = strings.TrimSpace(value)
	if t, err := time.Parse(time.DateOnly, value); err == nil {
		return when{date: true, t: t}, nil
	} else if t, err := time.Parse(time.RFC3339, value); err == nil {
		return when{t: t, zone: zone}, nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if zone == "" {
			break
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			break
		}
		if t, err := time.ParseInLocation(layout, value, loc); err == nil {
			return when{t: t, zone: zone}, nil
		}
	}
	return when{}, gofiler.ErrBadParameter.Withf("event: %s is a day, YYYY-MM-DD, or a time, of RFC 3339, or YYYY-MM-DDTHH:MM with a time zone, not %q", name, value)
}

// currentWhen returns the start, or end, of an event
func currentWhen(t httpclient.EventTime) (when, error) {
	if t.Date != "" {
		day, err := time.Parse(time.DateOnly, t.Date)
		return when{date: true, t: day}, err
	}
	at, err := time.Parse(time.RFC3339, t.DateTime)
	return when{t: at, zone: t.TimeZone}, err
}

// field returns the start, or end, of an event, as the API has it, with
// what it isn't of as null, when it's cleared, as a patch keeps what isn't
// written: the time, and zone, of a day, or the day of a time
func (w when) field(clear bool) map[string]any {
	field := map[string]any{}
	if clear {
		field["date"], field["dateTime"], field["timeZone"] = nil, nil, nil
	}
	if w.date {
		field["date"] = w.t.Format(time.DateOnly)
		return field
	}
	field["dateTime"] = w.t.Format(time.RFC3339)
	if w.zone != "" {
		field["timeZone"] = w.zone
	}
	return field
}
