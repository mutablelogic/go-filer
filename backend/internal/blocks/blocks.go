// Package blocks reads a file at offsets, of the blocks of it which are
// requested, the last of which are kept, so a parser's reads, which are
// scattered across a file, such as a PDF's objects, are of few requests
package blocks

import (
	"io"
	"slices"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// FetchFunc reads the file from the offset into the buffer, all of which is
// within the file, of one request
type FetchFunc func(p []byte, off int64) error

// Reader reads a file at offsets, of its blocks, which are requested of the
// function, and kept, the last Blocks of them, which is safe to call at once
type Reader struct {
	mu     sync.Mutex
	size   int64
	fetch  FetchFunc
	blocks []block // the blocks which are kept, the last read last
	kept   int64   // the bytes of the blocks which are kept
}

// block is a block of the file, from its offset
type block struct {
	start int64
	data  []byte
}

var _ io.ReaderAt = (*Reader)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Size is the size of a block, whose offset is a multiple of it, so
	// reads near each other are of one block
	Size = 512 << 10

	// Blocks is how many blocks are kept, of each file which is read, as
	// bytes, so at most Blocks * Size, 8 MB, is kept, apart from one read
	// which is more, which is kept alone
	Blocks = 16
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns a reader of a file of the size, whose blocks are requested of
// the function
func New(size int64, fetch FetchFunc) *Reader {
	return &Reader{size: size, fetch: fetch}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadAt reads the file at the offset, of the blocks which are kept, and
// requests those which aren't, together, as one block, which is kept, so a
// read which is more than a block is one request. A read beyond the end of
// the file is io.EOF, with what was read of it.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	} else if off >= r.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < r.size {
		b := r.find(off)
		if b == nil {
			var err error
			if b, err = r.request(off, int64(len(p)-n)); err != nil {
				return n, err
			}
		}
		copied := copy(p[n:], b.data[off-b.start:])
		n += copied
		off += int64(copied)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// find returns the block which is kept of the offset, which is made the
// last which is read, or nil
func (r *Reader) find(off int64) *block {
	for i, b := range r.blocks {
		if off >= b.start && off < b.start+int64(len(b.data)) {
			r.blocks = append(slices.Delete(r.blocks, i, i+1), b)
			return &r.blocks[len(r.blocks)-1]
		}
	}
	return nil
}

// request requests the blocks of the offset, from the start of its block,
// to the end of the block of the last byte which is wanted, or the end of
// the file, as one block, which is kept, in place of the one read least
// recently, until what's kept, with it, is at most Blocks * Size
func (r *Reader) request(off, want int64) (*block, error) {
	start := off - off%Size
	end := min(off+want, r.size)
	if rem := end % Size; rem != 0 {
		end = min(end+Size-rem, r.size)
	}
	data := make([]byte, end-start)
	if err := r.fetch(data, start); err != nil {
		return nil, err
	}
	for len(r.blocks) > 0 && r.kept+int64(len(data)) > Blocks*Size {
		r.kept -= int64(len(r.blocks[0].data))
		r.blocks = slices.Delete(r.blocks, 0, 1)
	}
	r.blocks = append(r.blocks, block{start: start, data: data})
	r.kept += int64(len(data))
	return &r.blocks[len(r.blocks)-1], nil
}
