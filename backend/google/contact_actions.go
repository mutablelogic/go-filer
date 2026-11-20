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

// contactAction creates, updates or deletes a contact
type contactAction struct {
	*ContactFiler
}

// contactInput is the input of the contact action. Its fields which are
// nil aren't changed, of an update, and an empty one is cleared. A list,
// such as of emails, replaces the contact's.
type contactInput struct {
	Op           string    `json:"op" enum:"create,update,delete" help:"What's done: create a contact, update one, or delete one"`
	Path         string    `json:"path,omitempty" help:"Path of the contact to update or delete, such as /<id>.vcf, which a contact which is created hasn't"`
	GivenName    *string   `json:"given_name,omitempty" help:"Given name"`
	FamilyName   *string   `json:"family_name,omitempty" help:"Family name"`
	Emails       *[]string `json:"emails,omitempty" help:"Email addresses, the first of which is the primary one"`
	Phones       *[]string `json:"phones,omitempty" help:"Phone numbers, the first of which is the primary one"`
	Organization *string   `json:"organization,omitempty" help:"Organization"`
	JobTitle     *string   `json:"job_title,omitempty" help:"Job title, at the organization"`
	Birthday     *string   `json:"birthday,omitempty" help:"Birthday, as YYYY-MM-DD, or --MM-DD without a year"`
	Notes        *string   `json:"notes,omitempty" help:"Notes"`
	Addresses    *[]string `json:"addresses,omitempty" help:"Postal addresses, each of one line, the first of which is the primary one"`
}

var _ gofiler.Action = contactAction{}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns what the volume can do, beyond its objects, when it's
// writable: contact, which creates, updates or deletes a contact
func (self *ContactFiler) Actions() []gofiler.Action {
	if self.readonly {
		return nil
	}
	return []gofiler.Action{contactAction{self}}
}

func (contactAction) Name() string {
	return "contact"
}

func (contactAction) Title() string {
	return "Create, update or delete a contact"
}

func (contactAction) Description() string {
	return "Creates a contact, of its fields, or, of the path of a contact, such as /<id>.vcf, updates the fields which are given, or deletes it. A list, such as of emails, replaces the contact's. Returns the contact."
}

func (contactAction) Input() *jsonschema.Schema {
	return jsonschema.MustFor[contactInput]()
}

// Run creates a contact, or updates, or deletes, the contact of the path,
// and returns it, as it is, or was, before it's deleted. The contacts are
// got again, so the change is listed, and the watch sends it when it polls.
func (self contactAction) Run(ctx context.Context, data json.RawMessage) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	var input contactInput
	if err := decodeInput("contact", data, &input); err != nil {
		return nil, err
	}
	id, err := input.check()
	if err != nil {
		return nil, err
	}
	defer self.people.clear()

	// A contact which is created is of its fields alone
	if input.Op == opCreate {
		person, updated, err := input.person(nil)
		if err != nil {
			return nil, err
		} else if len(updated) == 0 {
			return nil, gofiler.ErrBadParameter.With("contact: create needs a field")
		}
		created, err := self.client.CreateContact(ctx, person, httpclient.PersonFields)
		if err != nil {
			return nil, self.apiError("contact", "/", err)
		}
		return self.object(ctx, created, "/")
	}

	// A contact which is updated, or deleted, is got first, of its entity
	// tag, and its fields, which those which are given are merged with
	current, err := self.client.Person(ctx, "people/"+id, httpclient.PersonFields)
	if err != nil {
		return nil, self.apiError("contact", input.Path, err)
	} else if contactId(current) != id || !isContact(current) || current.Metadata.Deleted {
		return nil, &fs.PathError{Op: "contact", Path: input.Path, Err: fs.ErrNotExist}
	}
	if input.Op == opDelete {
		object, err := self.object(ctx, current, input.Path)
		if err != nil {
			return nil, err
		} else if err := self.client.DeleteContact(ctx, current.ResourceName); err != nil {
			return nil, self.apiError("contact", input.Path, err)
		}
		return object, nil
	}
	person, updated, err := input.person(current)
	if err != nil {
		return nil, err
	}
	person.ETag = current.ETag
	changed, err := self.client.UpdateContact(ctx, current.ResourceName, person, updated, httpclient.PersonFields)
	if err != nil {
		return nil, self.apiError("contact", input.Path, err)
	}
	return self.object(ctx, changed, input.Path)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// object returns the object of a contact, or fs.ErrNotExist, when it isn't
// one
func (self contactAction) object(ctx context.Context, person *httpclient.Person, p string) (*schema.Object, error) {
	object, _ := self.contactObject(ctx, person, false)
	if object == nil {
		return nil, &fs.PathError{Op: "contact", Path: p, Err: fs.ErrNotExist}
	}
	return object, nil
}

// check returns the id of the contact of the input's path, which a contact
// which is created hasn't, or ErrBadParameter, when its fields aren't valid
// for its op
func (input contactInput) check() (string, error) {
	switch input.Op {
	case opCreate:
		if input.Path != "" {
			return "", gofiler.ErrBadParameter.With("contact: create has no path, as Google names the contact")
		}
		return "", nil
	case opUpdate, opDelete:
		if input.Path == "" {
			return "", gofiler.ErrBadParameter.Withf("contact: %s needs the path of a contact", input.Op)
		}
		id, isRoot, err := contactPath("contact", input.Path)
		if err != nil {
			return "", err
		} else if isRoot {
			return "", gofiler.ErrBadParameter.Withf("contact: %s's path is of a contact, such as /<id>%s", input.Op, contactExt)
		}
		fields := input.GivenName != nil || input.FamilyName != nil || input.Emails != nil || input.Phones != nil || input.Organization != nil || input.JobTitle != nil || input.Birthday != nil || input.Notes != nil || input.Addresses != nil
		if input.Op == opDelete && fields {
			return "", gofiler.ErrBadParameter.With("contact: delete has only a path")
		} else if input.Op == opUpdate && !fields {
			return "", gofiler.ErrBadParameter.With("contact: update needs a field to change")
		}
		return id, nil
	default:
		return "", gofiler.ErrBadParameter.Withf("contact: op is create, update or delete, not %q", input.Op)
	}
}

// person returns the person of the input's fields which are given, of the
// current contact, when it's updated, whose name and organization those
// which aren't given are kept of, and the fields which are updated, of
// their names in the People API
func (input contactInput) person(current *httpclient.Person) (httpclient.Person, []string, error) {
	var person httpclient.Person
	var updated []string
	trim := func(v *string) string { return strings.TrimSpace(*v) }

	// The name, of its given and family names, of the current one, which
	// Google displays of them
	if input.GivenName != nil || input.FamilyName != nil {
		var name httpclient.PersonName
		if current != nil && len(current.Names) > 0 {
			name = current.Names[0]
		}
		name.DisplayName, name.Metadata = "", httpclient.FieldMetadata{}
		if input.GivenName != nil {
			name.GivenName = trim(input.GivenName)
		}
		if input.FamilyName != nil {
			name.FamilyName = trim(input.FamilyName)
		}
		if name != (httpclient.PersonName{}) {
			person.Names = []httpclient.PersonName{name}
		}
		updated = append(updated, "names")
	}
	if input.Emails != nil {
		person.EmailAddresses = personValues(*input.Emails)
		updated = append(updated, "emailAddresses")
	}
	if input.Phones != nil {
		person.PhoneNumbers = personValues(*input.Phones)
		updated = append(updated, "phoneNumbers")
	}

	// The organization, and the job title in it, of the current one
	if input.Organization != nil || input.JobTitle != nil {
		var org httpclient.PersonOrg
		if current != nil && len(current.Organizations) > 0 {
			org = current.Organizations[0]
		}
		org.Metadata = httpclient.FieldMetadata{}
		if input.Organization != nil {
			org.Name = trim(input.Organization)
		}
		if input.JobTitle != nil {
			org.Title = trim(input.JobTitle)
		}
		if org != (httpclient.PersonOrg{}) {
			person.Organizations = []httpclient.PersonOrg{org}
		}
		updated = append(updated, "organizations")
	}
	if input.Birthday != nil {
		if birthday := trim(input.Birthday); birthday != "" {
			date, err := parseBirthday(birthday)
			if err != nil {
				return person, nil, err
			}
			person.Birthdays = []httpclient.PersonDate{date}
		}
		updated = append(updated, "birthdays")
	}
	if input.Notes != nil {
		if notes := trim(input.Notes); notes != "" {
			person.Biographies = []httpclient.PersonValue{{Value: notes}}
		}
		updated = append(updated, "biographies")
	}
	if input.Addresses != nil {
		for _, address := range *input.Addresses {
			if address = strings.TrimSpace(address); address != "" {
				person.Addresses = append(person.Addresses, httpclient.PersonAddr{FormattedValue: address})
			}
		}
		updated = append(updated, "addresses")
	}
	return person, updated, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// personValues returns the values, such as email addresses, which aren't empty
func personValues(list []string) []httpclient.PersonValue {
	var result []httpclient.PersonValue
	for _, value := range list {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, httpclient.PersonValue{Value: value})
		}
	}
	return result
}

// parseBirthday returns a birthday, of YYYY-MM-DD, or --MM-DD without a year
func parseBirthday(value string) (httpclient.PersonDate, error) {
	var date httpclient.PersonDate
	layout, s := time.DateOnly, value
	if rest, ok := strings.CutPrefix(value, "--"); ok {
		layout, s = "2006-01-02", "2000-"+rest
	}
	t, err := time.Parse(layout, s)
	if err != nil {
		return date, gofiler.ErrBadParameter.Withf("contact: birthday is YYYY-MM-DD, or --MM-DD, not %q", value)
	}
	if !strings.HasPrefix(value, "--") {
		date.Date.Year = t.Year()
	}
	date.Date.Month, date.Date.Day = int(t.Month()), t.Day()
	return date, nil
}
