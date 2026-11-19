package memory_test

import (
	"testing"

	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	memory "github.com/mutablelogic/go-filer/filer/store/memory"
	storetest "github.com/mutablelogic/go-filer/filer/store/storetest"
	require "github.com/stretchr/testify/require"
)

func Test_Store(t *testing.T) {
	storetest.Run(t, func(t *testing.T, opts ...store.Opt) store.Store {
		s, err := memory.New(opts...)
		require.NoError(t, err)
		return s
	})
}
