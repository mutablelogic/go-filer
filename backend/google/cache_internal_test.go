package google

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	now := time.Now()
	newTestCache := func() *cache[int] {
		return newCache[int](func() time.Time { return now }, func() time.Duration { return time.Minute })
	}

	t.Run("Clear doesn't wait for a value being got", func(t *testing.T) {
		c := newTestCache()
		started, release := make(chan struct{}), make(chan struct{})
		go c.get(context.Background(), "/", func() (int, error) {
			close(started)
			<-release
			return 1, nil
		})
		<-started
		cleared := make(chan struct{})
		go func() {
			c.clear()
			close(cleared)
		}()
		select {
		case <-cleared:
		case <-time.After(time.Second):
			t.Fatal("clear waited for the value being got")
		}
		close(release)
	})

	t.Run("A value got while it's cleared isn't kept", func(t *testing.T) {
		c := newTestCache()
		if _, err := c.get(context.Background(), "/", func() (int, error) {
			c.clear()
			return 1, nil
		}); err != nil {
			t.Fatal(err)
		} else if _, exists := c.peek("/"); exists {
			t.Fatal("expected the value not to be kept")
		}
		if value, err := c.get(context.Background(), "/", func() (int, error) { return 2, nil }); err != nil || value != 2 {
			t.Fatalf("expected 2, got %v, %v", value, err)
		} else if value, exists := c.peek("/"); !exists || value != 2 {
			t.Fatalf("expected 2 to be kept, got %v, %v", value, exists)
		}
	})

	t.Run("A value asked for after it's cleared is got again", func(t *testing.T) {
		c := newTestCache()
		started, release := make(chan struct{}), make(chan struct{})
		stale := make(chan int)
		go func() {
			value, _ := c.get(context.Background(), "/", func() (int, error) {
				close(started)
				<-release
				return 1, nil
			})
			stale <- value
		}()
		<-started
		c.clear()
		if value, err := c.get(context.Background(), "/", func() (int, error) { return 2, nil }); err != nil || value != 2 {
			t.Fatalf("expected 2, got %v, %v", value, err)
		}
		close(release)
		if value := <-stale; value != 1 {
			t.Fatalf("expected 1, got %v", value)
		} else if value, exists := c.peek("/"); !exists || value != 2 {
			t.Fatalf("expected 2 to be kept, got %v, %v", value, exists)
		}
	})

	t.Run("A value is got once, however many ask for it", func(t *testing.T) {
		c := newTestCache()
		var calls atomic.Int32
		release := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if value, err := c.get(context.Background(), "/", func() (int, error) {
					calls.Add(1)
					<-release
					return 1, nil
				}); err != nil || value != 1 {
					t.Errorf("expected 1, got %v, %v", value, err)
				}
			})
		}
		time.Sleep(50 * time.Millisecond)
		close(release)
		wg.Wait()
		if n := calls.Load(); n != 1 {
			t.Fatalf("expected the value to be got once, got %d", n)
		}
	})

	t.Run("A value which couldn't be got is got by those waiting for it", func(t *testing.T) {
		c := newTestCache()
		started, release := make(chan struct{}), make(chan struct{})
		failed := make(chan error)
		go func() {
			_, err := c.get(context.Background(), "/", func() (int, error) {
				close(started)
				<-release
				return 0, errors.New("cancelled")
			})
			failed <- err
		}()
		<-started
		got := make(chan int)
		go func() {
			value, _ := c.get(context.Background(), "/", func() (int, error) { return 2, nil })
			got <- value
		}()
		time.Sleep(50 * time.Millisecond)
		close(release)
		if err := <-failed; err == nil {
			t.Fatal("expected an error")
		} else if value := <-got; value != 2 {
			t.Fatalf("expected 2, got %v", value)
		}
	})

	t.Run("A value whose getting panicked isn't returned to those waiting", func(t *testing.T) {
		c := newTestCache()
		started, release := make(chan struct{}), make(chan struct{})
		go func() {
			defer func() { recover() }()
			c.get(context.Background(), "/", func() (int, error) {
				close(started)
				<-release
				panic("panicked")
			})
		}()
		<-started
		got := make(chan int)
		go func() {
			value, _ := c.get(context.Background(), "/", func() (int, error) { return 3, nil })
			got <- value
		}()
		time.Sleep(50 * time.Millisecond)
		close(release)
		if value := <-got; value != 3 {
			t.Fatalf("expected 3, got %v", value)
		}
	})

	t.Run("A caller waiting for a value which is being got is cancelled", func(t *testing.T) {
		c := newTestCache()
		started, release := make(chan struct{}), make(chan struct{})
		loaded := make(chan int)
		go func() {
			value, _ := c.get(context.Background(), "/", func() (int, error) {
				close(started)
				<-release
				return 1, nil
			})
			loaded <- value
		}()
		<-started
		ctx, cancel := context.WithCancel(context.Background())
		waited := make(chan error)
		go func() {
			_, err := c.get(ctx, "/", func() (int, error) { return 2, nil })
			waited <- err
		}()
		cancel()
		select {
		case err := <-waited:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected the context's error, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("the caller wasn't cancelled")
		}
		close(release)
		if value := <-loaded; value != 1 {
			t.Fatalf("what gets it isn't cancelled: expected 1, got %v", value)
		}
	})

	t.Run("A cache which keeps values for no time keeps none", func(t *testing.T) {
		c := newCache[int](func() time.Time { return now }, func() time.Duration { return 0 })
		if _, err := c.get(context.Background(), "/", func() (int, error) { return 1, nil }); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		kept := len(c.values)
		c.mu.Unlock()
		if kept != 0 {
			t.Fatalf("expected none to be kept, got %d", kept)
		}
	})
}
