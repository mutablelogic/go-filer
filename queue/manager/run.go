package manager

import (
	"context"
	"log/slog"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	// Timer for next ticker
	timer_retries, timer_period := 0, schema.DefaultTickerPeriod
	timer := time.NewTimer(timer_period)
	defer timer.Stop()

	// Create a channel for ticker task results
	results := make(chan *Result, 16)
	defer close(results)

	for {
		select {
		case <-ctx.Done():
			// Wait on execution of any in-flight tasks to complete before returning
			manager.tickers.Close()
			manager.queues.Close()

			// Return success
			return nil
		case result := <-results:
			if result != nil && result.Error != nil {
				log.ErrorContext(ctx, "RunTickerTask result failed", "ticker", result.Ticker, "error", result.Error.Error())
			} else {
				log.InfoContext(ctx, "RunTickerTask result", "ticker", result.Ticker, "result", result)
			}
		case <-timer.C:
			// Otel span
			child, endSpan := otel.StartSpan(manager.tracer, ctx, "tick")

			// Get matured ticker
			ticker, err := manager.NextTicker(child)
			if err != nil {
				log.ErrorContext(ctx, "NextTicker failed", "error", err.Error())
			} else if ticker == nil {
				// Do nothing - no ticker matured
			} else {
				if err = manager.tickers.RunTickerTask(child, ticker, results); err != nil {
					log.ErrorContext(ctx, "RunTickerTask failed", "ticker", ticker, "error", err.Error())
				}
			}

			// Otel span
			endSpan(err)

			// Next tick
			timer_retries, timer_period = backoffPeriod(timer_retries, schema.DefaultTickerPeriod, err != nil)
			timer.Reset(timer_period)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func backoffPeriod(retries int, dur time.Duration, err bool) (int, time.Duration) {
	const maxRetries = 5
	if err {
		nextRetries := retries + 1
		if nextRetries > maxRetries {
			nextRetries = maxRetries
		}
		return nextRetries, time.Duration(nextRetries*nextRetries) * dur
	}
	return 0, dur
}
