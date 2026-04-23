package manager

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
)

type queueNotification struct {
	Schema string `json:"schema"`
	Queue  string `json:"queue"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	// Timer for next ticker
	timer_retries, timer_period := 0, schema.DefaultTickerPeriod
	timer := time.NewTimer(timer_period)
	defer timer.Stop()

	// Notification of new tasks in queues
	notify_ctx, notify_cancel := context.WithCancel(ctx)
	defer notify_cancel()
	notifications, err := manager.Subscribe(notify_ctx, schema.DefaultNotifyChannel)
	if err != nil {
		return err
	}

	// Shared completion channel for ticker callbacks
	results := make(chan *Result, 16)
	defer close(results)

	// Shutdown handling
	ctxDone := ctx.Done()
	timerC := timer.C
	shutdownDone := make(chan struct{})

	// Tick handler
	tick := func(trigger string) (err error) {
		// Otel span
		child, endSpan := otel.StartSpan(manager.tracer, ctx, strings.Join([]string{"tick", trigger}, "."))
		defer func() { endSpan(err) }()

		// Get matured ticker
		ticker, err := manager.NextTicker(child)
		if err != nil {
			log.ErrorContext(ctx, "NextTicker failed", "trigger", trigger, "error", err.Error())
			return err
		} else if ticker == nil {
			return nil
		} else if err = manager.tickers.RunTickerTask(child, ticker, results); err != nil {
			log.ErrorContext(ctx, "RunTickerTask failed", "trigger", trigger, "ticker", ticker, "error", err.Error())
			return err
		}
		return nil
	}

	// The run loop
	var wg sync.WaitGroup
	for {
		select {
		case <-ctxDone:
			// Stop scheduling new work, but keep the loop alive to drain in-flight results.
			ctxDone = nil
			timerC = nil
			notify_cancel()

			// Wait for remaining tasks in the background to finish before returning from Run.
			wg.Go(func() {
				manager.tickers.Close()
				manager.queues.Close()
				close(shutdownDone)
			})
		case <-shutdownDone:
			wg.Wait()
			return nil
		case notification, ok := <-notifications:
			if !ok {
				notifications = nil
				continue
			}
			if payload := decodeNotification(notification); payload != nil {
				log.DebugContext(ctx, "Got notification", "channel", notification.Channel, "schema", payload.Schema, "queue", payload.Queue)
				// TODO: No ticker matured, so this is the next place to try queue work.
				// The intended flow is:
				// 1. Decode the notification payload and/or use a worker name to call NextTask.
				// 2. If a task is retained, call manager.queues.RunQueueTask(child, task, results).
				// 3. When a queue result comes back on results, call ReleaseTask with success/failure
				//    and propagate the callback result payload.
			}
		case result := <-results:
			// TODO: When queue execution is wired in, branch on result.TaskId/result.Queue
			// here and release the retained task with ReleaseTask before logging.
			if result != nil && result.Error != nil {
				log.ErrorContext(ctx, "RunTickerTask result failed", "ticker", result.Ticker, "error", result.Error.Error())
			} else {
				log.InfoContext(ctx, "RunTickerTask result", "ticker", result.Ticker, "result", result)
			}
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

func decodeNotification(notification pg.Notification) *queueNotification {
	var payload queueNotification
	if err := json.Unmarshal(notification.Payload, &payload); err != nil {
		return nil
	}
	return &payload
}
