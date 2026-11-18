package memory_test

import (
	"testing"

	// Packages
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	memory "github.com/mutablelogic/go-filer/filer/indexer/memory"
	storetest "github.com/mutablelogic/go-filer/filer/indexer/storetest"
)

func Test_Store(t *testing.T) {
	storetest.Run(t, func(t *testing.T) indexer.Store { return memory.New() })
}
