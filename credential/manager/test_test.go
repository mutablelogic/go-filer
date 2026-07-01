package manager_test

import (
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-filer/credential/manager"
	test "github.com/mutablelogic/go-filer/credential/test"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func TestMain(m *testing.M) {
	test.Main(m, nil, manager.WithPassphrase(1, "test-passphrase"))
}
