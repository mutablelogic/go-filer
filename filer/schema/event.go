package schema

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

// Event names used with httpresponse.TextStream for long-running operations
// (e.g. reindexing) that report progress over a text/event-stream response.
const (
	EventProgress = "progress"
	EventError    = "error"
	EventDone     = "done"
)
