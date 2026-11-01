package schema

import "slices"

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Event types
type EventType int

// Event represents a change to an object in the backend.
type Event struct {
	EventType `json:"type"`
	ObjectKey `json:"key"`
}

// EventFilter is the changes which a subscription is sent, which are those
// of the volumes, or every volume, when there are none
type EventFilter struct {
	Name []string `json:"name,omitempty" help:"Volumes, which is every volume when there are none"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	EventTypeNone EventType = iota

	// An object which is created, or moved or renamed to the path
	EventTypeCreated

	// An object whose content or attributes changed
	EventTypeUpdated

	// An object which is deleted, or moved or renamed from the path
	EventTypeDeleted

	// A directory whose changes may have been missed, so the objects
	// beneath it should be compared with what's known about them
	EventTypeRescan
)

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t EventType) String() string {
	switch t {
	case EventTypeCreated:
		return "created"
	case EventTypeUpdated:
		return "updated"
	case EventTypeDeleted:
		return "deleted"
	case EventTypeRescan:
		return "rescan"
	default:
		return "none"
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Match returns true when the change is of one of the filter's volumes, or
// the filter has none
func (f EventFilter) Match(event Event) bool {
	return len(f.Name) == 0 || slices.Contains(f.Name, event.Name)
}
