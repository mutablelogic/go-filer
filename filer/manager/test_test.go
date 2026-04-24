package manager

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	// Packages
	queuemanager "github.com/mutablelogic/go-filer/queue/manager"
	pg "github.com/mutablelogic/go-pg"
	pgtest "github.com/mutablelogic/go-pg/pkg/test"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	sharedPool      pg.PoolConn
	testSchemaCount uint64
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func TestMain(m *testing.M) {
	pgtest.Main(m, func(pool pg.PoolConn) (func(), error) {
		sharedPool = pool
		return func() { sharedPool = nil }, nil
	})
}

func newTestManager(ctx context.Context, opts ...Opt) (*Manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sharedPool == nil {
		return nil, fmt.Errorf("test pool is not initialized")
	}

	queueSchema := fmt.Sprintf("queue_test_%d", atomic.AddUint64(&testSchemaCount, 1))
	queue, err := queuemanager.New(ctx, sharedPool, "queue", "test", queuemanager.WithSchema(queueSchema))
	if err != nil {
		return nil, err
	}

	return New(ctx, sharedPool, queue, opts...)
}
