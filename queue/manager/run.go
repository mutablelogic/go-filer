package manager

import (
	"context"
	"log/slog"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	period, retries := schema.DefaultTickerPeriod, 0
	timer := time.NewTimer(period)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			// Get matured ticker
			ticker, err := manager.NextTicker(ctx)
			if err != nil {
				log.ErrorContext(ctx, "NextTicker failed", "error", err)
			}

			// Fired ticker
			if ticker != nil {
				log.DebugContext(ctx, "fired", "ticker", ticker)
			}

			// Next tick
			retries, period = backoffPeriod(retries, err != nil)
			timer.Reset(period)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func backoffPeriod(retries int, err bool) (int, time.Duration) {
	const maxRetries = 5
	if err {
		nextRetries := retries + 1
		if nextRetries > maxRetries {
			nextRetries = maxRetries
		}
		return nextRetries, time.Duration(nextRetries*nextRetries) * schema.DefaultTickerPeriod
	}
	return 0, schema.DefaultTickerPeriod
}
