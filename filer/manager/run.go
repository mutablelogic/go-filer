package manager

import (
	"context"
	"log/slog"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	broadcaster "github.com/mutablelogic/go-pg/pkg/broadcaster"
	types "github.com/mutablelogic/go-server/pkg/types"
)

func (manager *Manager) Run(ctx context.Context, logger *slog.Logger) error {
	// Create a broadcaster for listening for events
	events, err := broadcaster.NewBroadcaster(manager.PoolConn, schema.NotifyChannel)
	if err != nil {
		return err
	}
	defer events.Close()

	// Subscribe to events and forward them to the volumeChange channel
	volumeChange := make(chan broadcaster.ChangeNotification, 100)
	defer close(volumeChange)
	if err := events.Subscribe(ctx, func(event broadcaster.ChangeNotification) {
		switch event.Table {
		case "volume":
			volumeChange <- event
		default:
			logger.DebugContext(ctx, "ignoring event", "event", types.Stringify(event))
		}
	}); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-volumeChange:
			// Handle an insert, update or delete for a volume
			logger.DebugContext(ctx, "Event", "event", types.Stringify(event))
		}
	}
}
