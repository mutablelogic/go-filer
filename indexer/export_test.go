package indexer

import "time"

// RecountInterval is recountInterval, of the tests
const RecountInterval = recountInterval

// SetTaskTimeout sets taskTimeout, of a test, and returns a function which
// sets it back
func SetTaskTimeout(d time.Duration) func() {
	previous := taskTimeout
	taskTimeout = d
	return func() { taskTimeout = previous }
}

// SetKindsRefresh sets kindsRefresh, of a test, and returns a function which
// sets it back
func SetKindsRefresh(d time.Duration) func() {
	previous := kindsRefresh
	kindsRefresh = d
	return func() { kindsRefresh = previous }
}
