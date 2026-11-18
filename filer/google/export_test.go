package google

import "testing"

// SetEndpoint sets the endpoint of the API, which is a test's, until it ends
func SetEndpoint(t testing.TB, endpoint string) {
	previous := tasksEndpoint
	tasksEndpoint = endpoint
	t.Cleanup(func() { tasksEndpoint = previous })
}
