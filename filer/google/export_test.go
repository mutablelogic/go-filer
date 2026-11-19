package google

import "testing"

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
