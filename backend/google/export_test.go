package google

import (
	"testing"
	"time"
)

// SetEndpoint sets the endpoint of the API, which is a test's, until it ends
func SetEndpoint(t testing.TB, endpoint string) {
	previous := tasksEndpoint
	tasksEndpoint = endpoint
	t.Cleanup(func() { tasksEndpoint = previous })
	SetUserinfoEndpoint(t, endpoint+"/userinfo")
}

// SetUserinfoEndpoint sets the endpoint of the user who signed in, which is
// a test's, until it ends
func SetUserinfoEndpoint(t testing.TB, endpoint string) {
	previous := userinfoEndpoint
	userinfoEndpoint = endpoint
	t.Cleanup(func() { userinfoEndpoint = previous })
}

// SetPeopleEndpoint sets the endpoint of the People API, which is a test's,
// until it ends
func SetPeopleEndpoint(t testing.TB, endpoint string) {
	previous := peopleEndpoint
	peopleEndpoint = endpoint
	t.Cleanup(func() { peopleEndpoint = previous })
	SetUserinfoEndpoint(t, endpoint+"/userinfo")
}

// SetCalendarEndpoint sets the endpoint of the Calendar API, which is a
// test's, until it ends
func SetCalendarEndpoint(t testing.TB, endpoint string) {
	previous := calendarEndpoint
	calendarEndpoint = endpoint
	t.Cleanup(func() { calendarEndpoint = previous })
	SetUserinfoEndpoint(t, endpoint+"/userinfo")
}

// SetCalendarNow sets the time of a calendar filer, which the months it
// lists are of
func SetCalendarNow(filer *CalendarFiler, now func() time.Time) {
	filer.now = now
}

// CalendarSyncKey returns the key of the sync tokens of a volume
func CalendarSyncKey(name string) string {
	return calendarSyncKey(name)
}
