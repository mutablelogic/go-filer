package manager_test

import (
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-filer/filer/manager"
	test "github.com/mutablelogic/go-filer/filer/test"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func TestMain(m *testing.M) {
	test.Main(m, nil, manager.WithPassphrase(1, "test-passphrase1"), manager.WithPassphrase(2, "test-passphrase2"), manager.WithIndexer(true))
}
