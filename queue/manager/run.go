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

	// Timer for next ticker
	timer_retries, timer_period := 0, schema.DefaultTickerPeriod
	timer := time.NewTimer(timer_period)
	defer timer.Stop()

	// Timer for adding partition tables (maintenance)
	maintenance_retries, maintenance_period := 0, schema.DefaultMaintenancePeriod
	maintenance := time.NewTimer(time.Second)
	defer maintenance.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-maintenance.C:
			// Create new partition tables if needed, and drop old drained partitions
			err := manager.maintain(ctx, log)
			if err != nil {
				log.ErrorContext(ctx, "maintenance failed", "error", err.Error())
			}

			// Next tick
			maintenance_retries, maintenance_period = backoffPeriod(maintenance_retries, schema.DefaultMaintenancePeriod, err != nil)
			maintenance.Reset(maintenance_period)
		case <-timer.C:
			// Get matured ticker
			ticker, err := manager.NextTicker(ctx)
			if err != nil {
				log.ErrorContext(ctx, "NextTicker failed", "error", err.Error())
			}

			// Fired ticker
			if ticker != nil {
				log.DebugContext(ctx, "fired", "ticker", ticker)
			}

			// Next tick
			timer_retries, timer_period = backoffPeriod(timer_retries, schema.DefaultTickerPeriod, err != nil)
			timer.Reset(timer_period)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) maintain(ctx context.Context, log *slog.Logger) error {
	// Create next partition if needed
	created, err := manager.CreateNextPartition(ctx)
	if err != nil {
		return err
	}
	if created != "" {
		log.InfoContext(ctx, "created partition", "partition", created)
	}

	// Drop old drained partitions
	dropped, err := manager.DropDrainedPartition(ctx)
	if err != nil {
		return err
	}
	if dropped != "" {
		log.InfoContext(ctx, "dropped partition", "partition", dropped)
	}

	return nil
}

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
