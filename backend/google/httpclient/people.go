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
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// PeopleClient is a client of the Google People API, of a person's
// contacts
type PeopleClient struct {
	*authclient.Client
}

// Person is a contact, of the fields which are asked for
type Person struct {
	ResourceName   string         `json:"resourceName,omitempty"`
	ETag           string         `json:"etag,omitempty"`
	Metadata       PersonMetadata `json:"metadata,omitzero"`
	Names          []PersonName   `json:"names,omitempty"`
	Nicknames      []PersonValue  `json:"nicknames,omitempty"`
	EmailAddresses []PersonValue  `json:"emailAddresses,omitempty"`
	PhoneNumbers   []PersonValue  `json:"phoneNumbers,omitempty"`
	URLs           []PersonValue  `json:"urls,omitempty"`
	Addresses      []PersonAddr   `json:"addresses,omitempty"`
	Organizations  []PersonOrg    `json:"organizations,omitempty"`
	Birthdays      []PersonDate   `json:"birthdays,omitempty"`
	Biographies    []PersonValue  `json:"biographies,omitempty"`
	Photos         []PersonPhoto  `json:"photos,omitempty"`
}

// PersonMetadata is when a person's sources were updated, such as the
// contact, and whether it's deleted
type PersonMetadata struct {
	Sources []PersonSource `json:"sources,omitempty"`
	Deleted bool           `json:"deleted,omitempty"`
}

// PersonSource is a source of a person, such as a CONTACT, or a PROFILE
type PersonSource struct {
	Type       string    `json:"type,omitempty"`
	ID         string    `json:"id,omitempty"`
	ETag       string    `json:"etag,omitempty"`
	UpdateTime time.Time `json:"updateTime,omitzero"`
}

// FieldMetadata is whether a field is the person's primary one
type FieldMetadata struct {
	Primary bool `json:"primary,omitempty"`
}

// PersonName is the name of a person
type PersonName struct {
	Metadata        FieldMetadata `json:"metadata,omitzero"`
	DisplayName     string        `json:"displayName,omitempty"`
	FamilyName      string        `json:"familyName,omitempty"`
	GivenName       string        `json:"givenName,omitempty"`
	MiddleName      string        `json:"middleName,omitempty"`
	HonorificPrefix string        `json:"honorificPrefix,omitempty"`
	HonorificSuffix string        `json:"honorificSuffix,omitempty"`
}

// PersonValue is a value of a person, such as an email address, a phone
// number, a URL, a nickname, or a biography, with its type, such as home
type PersonValue struct {
	Metadata FieldMetadata `json:"metadata,omitzero"`
	Value    string        `json:"value,omitempty"`
	Type     string        `json:"type,omitempty"`
}

// PersonAddr is a postal address of a person
type PersonAddr struct {
	Metadata        FieldMetadata `json:"metadata,omitzero"`
	FormattedValue  string        `json:"formattedValue,omitempty"`
	Type            string        `json:"type,omitempty"`
	POBox           string        `json:"poBox,omitempty"`
	ExtendedAddress string        `json:"extendedAddress,omitempty"`
	StreetAddress   string        `json:"streetAddress,omitempty"`
	City            string        `json:"city,omitempty"`
	Region          string        `json:"region,omitempty"`
	PostalCode      string        `json:"postalCode,omitempty"`
	Country         string        `json:"country,omitempty"`
}

// PersonOrg is an organisation of a person, with their title in it
type PersonOrg struct {
	Metadata   FieldMetadata `json:"metadata,omitzero"`
	Name       string        `json:"name,omitempty"`
	Department string        `json:"department,omitempty"`
	Title      string        `json:"title,omitempty"`
}

// PersonDate is a date of a person, such as a birthday, whose year is zero
// when it isn't known
type PersonDate struct {
	Metadata FieldMetadata `json:"metadata,omitzero"`
	Date     struct {
		Year  int `json:"year,omitempty"`
		Month int `json:"month,omitempty"`
		Day   int `json:"day,omitempty"`
	} `json:"date,omitzero"`
	Text string `json:"text,omitempty"`
}

// PersonPhoto is a photo of a person, which is a link, and whether it's
// the default, of their initials, rather than one of them
type PersonPhoto struct {
	Metadata FieldMetadata `json:"metadata,omitzero"`
	URL      string        `json:"url,omitempty"`
	Default  bool          `json:"default,omitempty"`
}

// connections is a page of a person's contacts, the last of which, of a
// sync, has the token of the changes after them
type connections struct {
	Connections   []Person `json:"connections,omitempty"`
	NextPageToken string   `json:"nextPageToken,omitempty"`
	NextSyncToken string   `json:"nextSyncToken,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// PeopleEndpoint is the endpoint of the Google People API
	PeopleEndpoint = "https://people.googleapis.com/v1"

	// ContactsScope is the scope of reading and writing contacts, and
	// ContactsReadonlyScope of reading them
	ContactsScope         = "https://www.googleapis.com/auth/contacts"
	ContactsReadonlyScope = "https://www.googleapis.com/auth/contacts.readonly"

	// PersonFields are the fields of a contact which are read, as a vCard
	PersonFields = "metadata,names,nicknames,emailAddresses,phoneNumbers,urls,addresses,organizations,birthdays,biographies,photos"

	// peoplePageSize is the most contacts of each page, which is the API's
	// most
	peoplePageSize = 1000
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewPeopleClient returns a client of the Google People API, at its
// endpoint, or PeopleEndpoint, when it's empty, with go-client's options,
// such as a token, a tracer, or a rate limit
func NewPeopleClient(endpoint string, opts ...client.ClientOpt) (*PeopleClient, error) {
	if endpoint = strings.TrimSpace(endpoint); endpoint == "" {
		endpoint = PeopleEndpoint
	}
	c, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &PeopleClient{Client: c}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Check returns nil when the contacts can be read, or an AuthError, when a
// sign-in is needed
func (c *PeopleClient) Check(ctx context.Context) error {
	var page connections
	return c.DoAuthWithContext(ctx, client.NewRequest(), &page,
		client.OptPath("people", "me", "connections"),
		client.OptQuery(url.Values{"pageSize": {"1"}, "personFields": {"metadata"}}),
	)
}

// Connections returns the contacts, of the fields, such as PersonFields,
// or "metadata", of their entity tags, a page at a time
func (c *PeopleClient) Connections(ctx context.Context, fields string) ([]Person, error) {
	var result []Person
	query := url.Values{"pageSize": {strconv.Itoa(peoplePageSize)}, "personFields": {fields}}
	for range maxPages {
		var page connections
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("people", "me", "connections"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		result = append(result, page.Connections...)
		if page.NextPageToken == "" {
			break
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return result, nil
}

// SyncConnections returns the contacts, of the fields, such as
// PersonFields, a page at a time, and the token of the changes after them,
// or, of a token, those which changed since, of which those which are
// deleted have metadata.deleted. Returns ErrSyncTokenGone when Google no
// longer accepts the token, such as one which is more than seven days old,
// when every contact is got again.
// Reference: https://developers.google.com/people/api/rest/v1/people.connections/list
func (c *PeopleClient) SyncConnections(ctx context.Context, fields, token string) ([]Person, string, error) {
	var result []Person
	query := url.Values{"pageSize": {strconv.Itoa(peoplePageSize)}, "personFields": {fields}, "requestSyncToken": {"true"}}
	if token != "" {
		query.Set("syncToken", token)
	}
	for range maxPages {
		var page connections
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("people", "me", "connections"),
			client.OptQuery(query),
		); errors.Is(err, httpresponse.Err(http.StatusGone)) {
			return nil, "", ErrSyncTokenGone
		} else if err != nil {
			return nil, "", err
		}
		result = append(result, page.Connections...)
		if page.NextPageToken != "" {
			query.Set("pageToken", page.NextPageToken)
			continue
		} else if page.NextSyncToken == "" {
			return nil, "", errors.New("no sync token was returned")
		}
		return result, page.NextSyncToken, nil
	}
	return nil, "", errTooManyPages
}

// Person returns a contact, by its resource name, such as people/c123, of
// the fields
func (c *PeopleClient) Person(ctx context.Context, resourceName, fields string) (*Person, error) {
	id, found := strings.CutPrefix(resourceName, "people/")
	if !found {
		id = resourceName
	}
	var result Person
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result,
		client.OptPath("people", id),
		client.OptQuery(url.Values{"personFields": {fields}}),
	); err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateContact creates a contact, of the person's fields, and returns it,
// of the fields, with the resource name Google gives it
func (c *PeopleClient) CreateContact(ctx context.Context, person Person, fields string) (*Person, error) {
	req, err := client.NewJSONRequestEx(http.MethodPost, person, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}
	var result Person
	if err := c.DoAuthWithContext(ctx, req, &result,
		client.OptPath("people:createContact"),
		client.OptQuery(url.Values{"personFields": {fields}}),
	); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateContact changes the fields of a contact, of its resource name,
// which are updated, such as names, of the person, whose entity tag must be
// the contact's, and returns it, of the fields
func (c *PeopleClient) UpdateContact(ctx context.Context, resourceName string, person Person, updated []string, fields string) (*Person, error) {
	id, _ := strings.CutPrefix(resourceName, "people/")
	req, err := client.NewJSONRequestEx(http.MethodPatch, person, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}
	var result Person
	if err := c.DoAuthWithContext(ctx, req, &result,
		client.OptPath("people", id+":updateContact"),
		client.OptQuery(url.Values{"updatePersonFields": {strings.Join(updated, ",")}, "personFields": {fields}}),
	); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteContact deletes a contact, of its resource name
func (c *PeopleClient) DeleteContact(ctx context.Context, resourceName string) error {
	id, _ := strings.CutPrefix(resourceName, "people/")
	return c.DoAuthWithContext(ctx, client.MethodDelete, nil,
		client.OptPath("people", id+":deleteContact"),
	)
}
