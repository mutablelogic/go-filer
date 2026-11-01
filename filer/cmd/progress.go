package cmd

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// transfer shows the progress of sending, or receiving, a number of bytes,
// on standard error, as a bar which is drawn again as the bytes are read,
// with the file which is being read, until it's closed, when it's cleared
type transfer struct {
	w   io.Writer // where it's drawn, which is standard error
	bar interface {
		Write(io.Writer, string, float64) (int, error)
	}
	total int64
	done  atomic.Int64
	name  atomic.Pointer[string]
	stop  chan struct{}
	wg    sync.WaitGroup
}

// transferReader is a reader whose bytes are counted by the transfer
type transferReader struct {
	io.ReadCloser
	t *transfer
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// transferInterval is how often the progress is drawn
const transferInterval = 100 * time.Millisecond

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newTransfer returns the progress of a transfer of total bytes, for a
// terminal of the width, or nil when standard error isn't a terminal, which
// is when the width is zero, when nothing is shown
func newTransfer(width int, total int64) *transfer {
	if width <= 0 {
		return nil
	}
	t := &transfer{w: os.Stderr, bar: tui.Progress(tui.SetWidth(width)), total: total, stop: make(chan struct{})}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(transferInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.draw()
			case <-t.stop:
				return
			}
		}
	}()
	return t
}

// Close stops drawing the progress, and clears it
func (t *transfer) Close() {
	if t == nil {
		return
	}
	close(t.stop)
	t.wg.Wait()
	fmt.Fprint(t.w, "\r\033[K")
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// Reader returns a reader of the file, by its name, whose bytes are counted,
// or the reader, without a transfer
func (t *transfer) Reader(name string, r io.ReadCloser) io.ReadCloser {
	if t == nil {
		return r
	}
	t.name.Store(&name)
	return &transferReader{ReadCloser: r, t: t}
}

func (r *transferReader) Read(data []byte) (int, error) {
	n, err := r.ReadCloser.Read(data)
	r.t.done.Add(int64(n))
	return n, err
}

// draw draws the progress again, over what was drawn
func (t *transfer) draw() {
	done, name := t.done.Load(), ""
	if p := t.name.Load(); p != nil {
		name = *p
	}
	percent := 100.0
	if t.total > 0 {
		percent = float64(done) * 100 / float64(t.total)
	}
	status := fmt.Sprintf("%s of %s %s", schema.FormatSize(done), schema.FormatSize(t.total), name)
	fmt.Fprint(t.w, "\r\033[K")
	t.bar.Write(t.w, status, percent)
}
