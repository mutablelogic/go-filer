package indexer

import (
	"context"
	"sync"
	"testing"
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
)

// pushes is a queue store which records the work which is pushed
type pushes struct {
	store.QueueStore
	sync.Mutex
	work []store.Work
}

func (p *pushes) Push(_ context.Context, _ string, work ...store.Work) error {
	p.Lock()
	defer p.Unlock()
	p.work = append(p.work, work...)
	return nil
}

func (p *pushes) last() store.Work {
	p.Lock()
	defer p.Unlock()
	return p.work[len(p.work)-1]
}

func Test_Queue(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	s := &pushes{}
	q := newQueue(s, "media", time.Second, time.Minute)
	q.now = func() time.Time { return now }

	// The index of a key is due after the debounce, and wakes the worker
	if err := q.index(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	if w := s.last(); w.Kind != store.KindIndex || w.Key != "/a" || !w.After.Equal(now.Add(time.Second)) {
		t.Errorf("index: %+v", w)
	}
	select {
	case <-q.wake:
	default:
		t.Error("the worker isn't woken")
	}

	// A rescan of a folder which hasn't been is due after the debounce
	if err := q.rescan(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if w := s.last(); w.Kind != store.KindRescan || !w.After.Equal(now.Add(time.Second)) {
		t.Errorf("rescan: %+v", w)
	}

	// Once it's rescanned, the next is deferred until the minimum interval
	// has passed, rather than dropped, of that folder only
	q.rescanned("/")
	now = now.Add(10 * time.Second)
	if err := q.rescan(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if w, want := s.last(), now.Add(50*time.Second); !w.After.Equal(want) {
		t.Errorf("deferred: got %v, want %v", w.After, want)
	}
	if err := q.rescan(ctx, "/a/"); err != nil {
		t.Fatal(err)
	}
	if w := s.last(); !w.After.Equal(now.Add(time.Second)) {
		t.Errorf("another folder: %v", w.After)
	}

	// Once the interval has passed, it's due after the debounce again
	now = now.Add(time.Minute)
	if err := q.rescan(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if w := s.last(); !w.After.Equal(now.Add(time.Second)) {
		t.Errorf("after the interval: %v", w.After)
	}
}
