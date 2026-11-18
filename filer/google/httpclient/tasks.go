// Package httpclient has clients of Google's APIs, with go-auth's
// auth/httpclient, so a request which isn't authorized returns an AuthError,
// which says a sign-in is needed.
package httpclient

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// TasksClient is a client of the Google Tasks API
type TasksClient struct {
	*authclient.Client
}

// TaskList is a list of tasks
type TaskList struct {
	Id      string    `json:"id"`
	ETag    string    `json:"etag,omitempty"`
	Title   string    `json:"title,omitempty"`
	Updated time.Time `json:"updated,omitzero"`
}

// Task is a task of a list. Its due date has no time, though it's a time,
// at midnight, in UTC.
type Task struct {
	Id          string     `json:"id"`
	ETag        string     `json:"etag,omitempty"`
	Title       string     `json:"title,omitempty"`
	Notes       string     `json:"notes,omitempty"`
	Status      string     `json:"status,omitempty"` // needsAction or completed
	Due         *time.Time `json:"due,omitempty"`
	Completed   *time.Time `json:"completed,omitempty"`
	Updated     time.Time  `json:"updated,omitzero"`
	Parent      string     `json:"parent,omitempty"`   // The task this is a subtask of
	Position    string     `json:"position,omitempty"` // Its position among its siblings
	Deleted     bool       `json:"deleted,omitempty"`
	Hidden      bool       `json:"hidden,omitempty"` // Completed, and cleared from the list
	Links       []TaskLink `json:"links,omitempty"`
	WebViewLink string     `json:"webViewLink,omitempty"`
}

// TaskLink is a link of a task, such as to the email it was created from
type TaskLink struct {
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	Link        string `json:"link,omitempty"`
}

// taskLists and tasks are pages of task lists and tasks
type taskLists struct {
	Items         []TaskList `json:"items,omitempty"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}

type tasks struct {
	Items         []Task `json:"items,omitempty"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// TasksEndpoint is the endpoint of the Google Tasks API
	TasksEndpoint = "https://tasks.googleapis.com/tasks/v1"

	// TasksScope is the scope of reading and writing tasks, and
	// TasksReadonlyScope of reading them
	TasksScope         = "https://www.googleapis.com/auth/tasks"
	TasksReadonlyScope = "https://www.googleapis.com/auth/tasks.readonly"

	// pageSize is the most task lists, or tasks, of each page, which is the
	// API's most
	pageSize = 100

	// maxPages is the most pages which are read of task lists, or a list's
	// tasks, so a server which always returns another page doesn't loop
	maxPages = 1000
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewTasksClient returns a client of the Google Tasks API, at its endpoint,
// or TasksEndpoint, when it's empty, with go-client's options, such as a
// token, a tracer, or a rate limit
func NewTasksClient(endpoint string, opts ...client.ClientOpt) (*TasksClient, error) {
	if endpoint = strings.TrimSpace(endpoint); endpoint == "" {
		endpoint = TasksEndpoint
	}
	c, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &TasksClient{Client: c}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Check returns nil when the task lists can be read, or an AuthError, when
// a sign-in is needed
func (c *TasksClient) Check(ctx context.Context) error {
	var page taskLists
	return c.DoAuthWithContext(ctx, client.NewRequest(), &page,
		client.OptPath("users", "@me", "lists"),
		client.OptQuery(url.Values{"maxResults": {"1"}}),
	)
}

// TaskLists returns all of the task lists, a page at a time
func (c *TasksClient) TaskLists(ctx context.Context) ([]TaskList, error) {
	var result []TaskList
	query := url.Values{"maxResults": {strconv.Itoa(pageSize)}}
	for range maxPages {
		var page taskLists
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("users", "@me", "lists"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.NextPageToken == "" {
			break
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return result, nil
}

// TaskList returns a task list, by its id
func (c *TasksClient) TaskList(ctx context.Context, id string) (*TaskList, error) {
	var result TaskList
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result,
		client.OptPath("users", "@me", "lists", id),
	); err != nil {
		return nil, err
	}
	return &result, nil
}

// Tasks returns all of the tasks of a list, a page at a time, including
// those which are completed, hidden, assigned, such as from Docs or Chat
// spaces, or deleted, which say they are, and when they were
func (c *TasksClient) Tasks(ctx context.Context, list string) ([]Task, error) {
	var result []Task
	query := url.Values{
		"maxResults":    {strconv.Itoa(pageSize)},
		"showCompleted": {"true"},
		"showHidden":    {"true"},
		"showAssigned":  {"true"},
		"showDeleted":   {"true"},
	}
	for range maxPages {
		var page tasks
		if err := c.DoAuthWithContext(ctx, client.NewRequest(), &page,
			client.OptPath("lists", list, "tasks"),
			client.OptQuery(query),
		); err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.NextPageToken == "" {
			break
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return result, nil
}

// Task returns a task of a list, by their ids
func (c *TasksClient) Task(ctx context.Context, list, id string) (*Task, error) {
	var result Task
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result,
		client.OptPath("lists", list, "tasks", id),
	); err != nil {
		return nil, err
	}
	return &result, nil
}
