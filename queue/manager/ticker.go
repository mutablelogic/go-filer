package manager

import (
	"context"
	"errors"

	// Packages
	schema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TICKER

// RegisterTicker creates a new ticker, or updates an existing ticker, and returns it.
func (manager *Manager) RegisterTicker(ctx context.Context, name string, meta schema.TickerMeta, callback schema.TaskFunc) (*schema.Ticker, error) {
	var result schema.Ticker

	// Register the ticker task
	if err := manager.tickers.RegisterTask(name, callback); err != nil {
		return nil, err
	}

	// Persist the ticker
	err := manager.Tx(ctx, func(conn pg.Conn) error {
		err := conn.Get(ctx, &result, schema.TickerName(name))
		switch {
		case err == nil:
			// Ticker already exists, update below.
		case errors.Is(err, pg.ErrNotFound):
			if err := conn.With("id", name).Insert(ctx, &result, meta); err != nil {
				return err
			}
		default:
			return err
		}

		if !hasTickerMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &result, schema.TickerName(name), meta)
	})
	if err != nil {
		return nil, errors.Join(err, manager.tickers.RemoveTask(name))
	}

	return types.Ptr(result), nil
}

// UpdateTicker updates an existing ticker, and returns it.
func (manager *Manager) UpdateTicker(ctx context.Context, name string, meta schema.TickerMeta) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.Update(ctx, &ticker, schema.TickerName(name), meta); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// GetTicker returns a ticker by name.
func (manager *Manager) GetTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.Get(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// DeleteTicker deletes an existing ticker, and returns the deleted ticker.
func (manager *Manager) DeleteTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.Delete(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	} else if err := manager.tickers.RemoveTask(name); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(ticker), nil
}

// ListTickers returns all tickers as a list.
func (manager *Manager) ListTickers(ctx context.Context, req schema.TickerListRequest) (*schema.TickerList, error) {
	result := schema.TickerList{TickerListRequest: req}
	if err := manager.List(ctx, &result, req); err != nil {
		return nil, err
	} else {
		result.OffsetLimit.Clamp(result.Count)
	}
	return types.Ptr(result), nil
}

// NextTicker returns the next matured ticker, or nil.
func (manager *Manager) NextTicker(ctx context.Context) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.Get(ctx, &ticker, schema.TickerNext{}); errors.Is(err, pg.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return types.Ptr(ticker), nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func hasTickerMetaPatch(meta schema.TickerMeta) bool {
	return meta.Interval != nil || len(meta.Payload) > 0
}
