package manager

import (
	"context"
	"log/slog"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	notifications, err := manager.Subscribe(ctx, schema.DefaultNotifyChannel)
	if err != nil {
		return err
	}

	// Timer for next ticker
	timer_retries, timer_period := 0, schema.DefaultTickerPeriod
	timer := time.NewTimer(timer_period)
	defer timer.Stop()
	timerC := timer.C
	ctxDone := ctx.Done()
	notifyC := notifications

	// Create a channel for ticker task results
	results := make(chan *Result, 16)
	defer close(results)
	shutdownDone := make(chan struct{})
	shuttingDown := false

	tick := func(trigger string) error {
		var tickErr error

		// Otel span
		child, endSpan := otel.StartSpan(manager.tracer, ctx, strings.Join([]string{"tick", trigger}, "."))
		defer func() { endSpan(tickErr) }()

		// Get matured ticker
		ticker, err := manager.NextTicker(child)
		if err != nil {
			tickErr = err
			log.ErrorContext(ctx, "NextTicker failed", "trigger", trigger, "error", err.Error())
			return err
		} else if ticker == nil {
			// TODO: Get a queue task
			return nil
		} else if err = manager.tickers.RunTickerTask(child, ticker, results); err != nil {
			tickErr = err
			log.ErrorContext(ctx, "RunTickerTask failed", "trigger", trigger, "ticker", ticker, "error", err.Error())
			return err
		}

		return nil
	}

	for {
		select {
		case <-ctxDone:
			if shuttingDown {
				continue
			}

			// Stop scheduling new work, but keep the loop alive to drain in-flight results.
			shuttingDown = true
			ctxDone = nil
			timerC = nil
			notifyC = nil

			go func() {
				manager.tickers.Close()
				manager.queues.Close()
				close(shutdownDone)
			}()
		case <-shutdownDone:
			return nil
		case result := <-results:
			if result != nil && result.Error != nil {
				log.ErrorContext(ctx, "RunTickerTask result failed", "ticker", result.Ticker, "error", result.Error.Error())
			} else {
				log.InfoContext(ctx, "RunTickerTask result", "ticker", result.Ticker, "result", result)
			}
		case _, ok := <-notifyC:
			// TODO
			if !ok {
				notifyC = nil
				continue
			}

			timer_retries, timer_period = backoffPeriod(timer_retries, schema.DefaultTickerPeriod, tick("notify") != nil)
			resetTimer(timer, timer_period)
		case <-timerC:
			timer_retries, timer_period = backoffPeriod(timer_retries, schema.DefaultTickerPeriod, tick("timer") != nil)
			resetTimer(timer, timer_period)
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

func resetTimer(timer *time.Timer, dur time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(dur)
}
