package httpclient

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// CalendarClient is a client of the Google Calendar API
type CalendarClient struct {
	*authclient.Client
}

// Calendar is a calendar of the user's calendar list
// Reference: https://developers.google.com/calendar/api/v3/reference/calendarList
type Calendar struct {
	Id              string `json:"id"`
	ETag            string `json:"etag,omitempty"`
	Summary         string `json:"summary,omitempty"`
	SummaryOverride string `json:"summaryOverride,omitempty"` // The name the user gave it
	TimeZone        string `json:"timeZone,omitempty"`
	Primary         bool   `json:"primary,omitempty"`
	Deleted         bool   `json:"deleted,omitempty"`
	AccessRole      string `json:"accessRole,omitempty"`
}

// Event is an event of a calendar, or an occurrence of one which repeats,
// whose RecurringEventId is the event's
// Reference: https://developers.google.com/calendar/api/v3/reference/events
type Event struct {
	Id                string        `json:"id"`
	ETag              string        `json:"etag,omitempty"`
	Status            string        `json:"status,omitempty"` // confirmed, tentative or cancelled
	HTMLLink          string        `json:"htmlLink,omitempty"`
	Created           time.Time     `json:"created,omitzero"`
	Updated           time.Time     `json:"updated,omitzero"`
	Summary           string        `json:"summary,omitempty"`
	Description       string        `json:"description,omitempty"`
	Location          string        `json:"location,omitempty"`
	Start             EventTime     `json:"start,omitzero"`
	End               EventTime     `json:"end,omitzero"`
	RecurringEventId  string        `json:"recurringEventId,omitempty"`
	OriginalStartTime EventTime     `json:"originalStartTime,omitzero"`
	Transparency      string        `json:"transparency,omitempty"` // opaque, or transparent, which isn't busy
	ICalUID           string        `json:"iCalUID,omitempty"`
	Sequence          int           `json:"sequence,omitempty"`
	Organizer         EventPerson   `json:"organizer,omitzero"`
	Attendees         []EventPerson `json:"attendees,omitempty"`
	EventType         string        `json:"eventType,omitempty"` // default, birthday, outOfOffice, focusTime, workingLocation, fromGmail
}

// EventTime is when an event starts or ends: a date, of an all-day event,
// or a time, with its offset, and its zone, when it's set
type EventTime struct {
	Date     string `json:"date,omitempty"`     // Such as 2026-11-20
	DateTime string `json:"dateTime,omitempty"` // Such as 2026-11-20T09:00:00+01:00
	TimeZone string `json:"timeZone,omitempty"` // Such as Europe/Berlin
}

// EventPerson is the organiser, or an attendee, of an event
type EventPerson struct {
	Email          string `json:"email,omitempty"`
	DisplayName    string `json:"displayName,omitempty"`
	Self           bool   `json:"self,omitempty"`
	ResponseStatus string `json:"responseStatus,omitempty"`
}

// calendars and events are pages of calendars and events
type calendars struct {
	Items         []Calendar `json:"items,omitempty"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}

type events struct {
	Items         []Event `json:"items,omitempty"`
	NextPageToken string  `json:"nextPageToken,omitempty"`
	NextSyncToken string  `json:"nextSyncToken,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// CalendarEndpoint is the endpoint of the Google Calendar API
	CalendarEndpoint = "https://www.googleapis.com/calendar/v3"

	// CalendarScope is the scope of reading and writing calendars, and
	// CalendarReadonlyScope of reading them
	CalendarScope         = "https://www.googleapis.com/auth/calendar"
	CalendarReadonlyScope = "https://www.googleapis.com/auth/calendar.readonly"

	// eventPageSize and calendarPageSize are the most events, and calendars,
	// of each page, which are the API's most
	eventPageSize    = 2500
	calendarPageSize = 250
)

// ErrSyncTokenGone is the error of a sync token which Google no longer
// accepts, as it's expired, or the calendar's changes since it can't be
// listed, so its events are synced again, for another
var ErrSyncTokenGone = errors.New("the sync token is gone")

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewCalendarClient returns a client of the Google Calendar API, at its
// endpoint, or CalendarEndpoint, when it's empty, with go-client's options
func NewCalendarClient(endpoint string, opts ...client.ClientOpt) (*CalendarClient, error) {
	if endpoint = strings.TrimSpace(endpoint); endpoint == "" {
		endpoint = CalendarEndpoint
	}
	c, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &CalendarClient{Client: c}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Check returns nil when the calendars can be read, or an AuthError, when a
// sign-in is needed
func (c *CalendarClient) Check(ctx context.Context) error {
	var page calendars
	return c.DoAuthWithContext(ctx, client.NewRequest(), &page,
		client.OptPath("users", "me", "calendarList"),
		client.OptQuery(url.Values{"maxResults": {"1"}}),
	)
}

// Calendars returns the calendars of the user's calendar list, a page at a
// time, without those which are deleted
func (c *CalendarClient) Calendars(ctx context.Context) ([]Calendar, error) {
	var result []Calendar
	query := url.Values{"maxResults": {strconv.Itoa(calendarPageSize)}}
	for range maxPages {
		var page calendars
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("users", "me", "calendarList"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		for _, calendar := range page.Items {
			if !calendar.Deleted {
				result = append(result, calendar)
			}
		}
		if page.NextPageToken == "" {
			break
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return result, nil
}

// Events returns the events of a calendar which are between the times, a
// page at a time, each occurrence of an event which repeats being an event
// of its own, without those which are cancelled
func (c *CalendarClient) Events(ctx context.Context, calendar string, from, to time.Time) ([]Event, error) {
	var result []Event
	query := url.Values{
		"maxResults":   {strconv.Itoa(eventPageSize)},
		"singleEvents": {"true"},
		"timeMin":      {from.UTC().Format(time.RFC3339)},
		"timeMax":      {to.UTC().Format(time.RFC3339)},
	}
	for range maxPages {
		var page events
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("calendars", calendar, "events"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		for _, event := range page.Items {
			if event.Status != "cancelled" {
				result = append(result, event)
			}
		}
		if page.NextPageToken == "" {
			break
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return result, nil
}

// Event returns an event of a calendar, or an occurrence of one, by their
// ids
func (c *CalendarClient) Event(ctx context.Context, calendar, id string) (*Event, error) {
	var result Event
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result,
		client.OptPath("calendars", calendar, "events", id),
	); err != nil {
		return nil, err
	}
	return &result, nil
}

// Sync returns how many of a calendar's events changed since the sync
// token, which are its events, of every time, without one, and the token
// of the changes after them. Only their identifiers are got, as what changed
// is listed again, of the times it's listed. Returns ErrSyncTokenGone when
// Google no longer accepts the token, so it's synced again without one.
// Reference: https://developers.google.com/calendar/api/guides/sync
func (c *CalendarClient) Sync(ctx context.Context, calendar, token string) (int, string, error) {
	query := url.Values{
		"maxResults": {strconv.Itoa(eventPageSize)},
		"fields":     {"items(id),nextPageToken,nextSyncToken"},
	}
	if token != "" {
		query.Set("syncToken", token)
	}
	changed := 0
	for range maxPages {
		var page events
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("calendars", calendar, "events"),
			client.OptQuery(query),
		); errors.Is(err, httpresponse.Err(http.StatusGone)) {
			return 0, "", ErrSyncTokenGone
		} else if err != nil {
			return 0, "", err
		}
		changed += len(page.Items)
		if page.NextPageToken == "" {
			return changed, page.NextSyncToken, nil
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return changed, "", nil
}
