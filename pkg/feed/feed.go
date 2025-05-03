package feed

import (
	"context"
	"errors"

	// Packages
	pg "github.com/djthorpe/go-pg"
	"github.com/mutablelogic/go-filer"
	handler "github.com/mutablelogic/go-filer/pkg/feed/handler"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	server "github.com/mutablelogic/go-server"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	conn pg.PoolConn
}

var _ filer.Feed = (*Manager)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewManager(ctx context.Context, conn pg.PoolConn, router server.HTTPRouter) (*Manager, error) {
	self := new(Manager)
	self.conn = conn.With(
		"schema", schema.SchemaName,
	).(pg.PoolConn)

	// Create the schema
	if exists, err := pg.SchemaExists(ctx, self.conn, schema.SchemaName); err != nil {
		return nil, err
	} else if !exists {
		if err := pg.SchemaCreate(ctx, self.conn, schema.SchemaName); err != nil {
			return nil, err
		}
	}

	// Router
	if router != nil {
		handler.RegisterHandlers(ctx, router, self)
	}

	// Bootstrap the schema
	if err := self.conn.Tx(ctx, func(conn pg.Conn) error {
		return schema.Bootstrap(ctx, conn)
	}); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - URL

func (manager *Manager) CreateUrl(ctx context.Context, meta schema.UrlMeta) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Insert(ctx, &url, meta); err != nil {
		return nil, err
	}
	// Return success
	return &url, nil
}

func (manager *Manager) ListUrls(ctx context.Context, req schema.UrlListRequest) (*schema.UrlList, error) {
	var list schema.UrlList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, httperr(err)
	} else {
		return &list, nil
	}
}

func (manager *Manager) GetUrl(ctx context.Context, id uint64) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Get(ctx, &url, schema.UrlId(id)); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

func (manager *Manager) DeleteUrl(ctx context.Context, id uint64) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Delete(ctx, &url, schema.UrlId(id)); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func httperr(err error) error {
	if errors.Is(err, pg.ErrNotFound) {
		return httpresponse.ErrNotFound.With(err)
	}
	return err
}
