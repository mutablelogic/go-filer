package manager

import (
	"context"
	"errors"
)

// Run all backends
func (manager *Manager) Run(ctx context.Context) (err error) {
	var result error

	// Wait for the context to be done
	<-ctx.Done()

	// Close all backends
	for _, backend := range manager.backends {
		if err := backend.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}
