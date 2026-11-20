package google

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// driveReader reads a file's content, of its revision when it was listed,
// from its offset, which can seek, and be read at an offset, so a file,
// such as a video, is read in place, of the ranges which are read, rather
// than all of it. What's read is requested from the offset, and is read as
// it's received, until it's sought elsewhere, when it's requested again,
// from there. ReadAt requests blocks of the file, apart from Read and
// Seek, of at least driveBlock, the last of which is kept, so reads near
// each other, such as a parser's of a file's headers, are of one request.
type driveReader struct {
	*DriveFiler
	ctx    context.Context
	path   string // The file's path, of its errors
	file   *httpclient.DriveFile
	size   int64
	offset int64        // Where the next read is from
	stream *driveStream // What's requested, from where it's read, or nil

	// The block which ReadAt read last, and where it's from
	mu         sync.Mutex
	block      []byte
	blockStart int64
}

// driveStream is the content of a file which is requested, from where it's
// read, as it's received, of a request which is cancelled when it's closed
type driveStream struct {
	at     int64
	r      *io.PipeReader
	cancel context.CancelFunc
	done   chan struct{}
}

var _ io.ReadSeekCloser = (*driveReader)(nil)
var _ io.ReaderAt = (*driveReader)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// driveBlock is the least of a file which ReadAt requests, so small reads
// near each other aren't each a request
const driveBlock = 512 << 10

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Read reads the content from the offset, which is requested, when it isn't,
// unless it's at the end of the file, or beyond it
func (r *driveReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	} else if r.offset >= r.size {
		return 0, io.EOF
	} else if r.stream == nil || r.stream.at != r.offset {
		if err := r.open(r.offset); err != nil {
			return 0, err
		}
	}
	n, err := r.stream.r.Read(p)
	r.offset += int64(n)
	r.stream.at += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		err = r.readError(err)
	}
	return n, err
}

// Seek changes the offset of the next read, from the start or end of the
// file, or the offset, which is requested when it's read
func (r *driveReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.offset
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, &fs.PathError{Op: "seek", Path: r.path, Err: fs.ErrInvalid}
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "seek", Path: r.path, Err: fs.ErrInvalid}
	}
	r.offset = offset
	return r.offset, nil
}

// ReadAt reads the content at the offset, which is requested of its range,
// at least driveBlock of it, which is kept, apart from Read and Seek, and
// is safe to call at once
func (r *driveReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	} else if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: r.path, Err: fs.ErrInvalid}
	} else if off >= r.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && off < r.size {
		if off < r.blockStart || off >= r.blockStart+int64(len(r.block)) {
			if err := r.fetch(off, int64(len(p)-n)); err != nil {
				return n, err
			}
		}
		copied := copy(p[n:], r.block[off-r.blockStart:])
		n += copied
		off += int64(copied)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Close cancels what's requested, if anything
func (r *driveReader) Close() error {
	if r.stream != nil {
		r.stream.close()
		r.stream = nil
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// open requests the content from the offset, in place of what's requested,
// and returns once it's received, or the error of requesting it
func (r *driveReader) open(offset int64) error {
	r.Close()
	ctx, cancel := context.WithCancel(r.ctx)
	pr, pw := io.Pipe()
	s := &driveStream{at: offset, r: pr, cancel: cancel, done: make(chan struct{})}
	started, failed := make(chan struct{}), make(chan error, 1)
	go func() {
		defer close(s.done)
		err := r.client.Download(ctx, r.file.Id, r.file.HeadRevision, offset, 0, func(body io.Reader) error {
			close(started)
			_, err := io.Copy(pw, body)
			return err
		})
		pw.CloseWithError(err)
		failed <- err
	}()
	select {
	case <-started:
		r.stream = s
		return nil
	case err := <-failed:
		s.close()
		if err == nil {
			// Nothing was received, which is the end
			err = io.ErrUnexpectedEOF
		}
		return r.readError(err)
	}
}

// fetch requests the block of the file from the offset, of at least
// driveBlock, or of want, when it's more, up to the end of the file, which
// must be locked
func (r *driveReader) fetch(off, want int64) error {
	length := min(off+max(want, driveBlock), r.size) - off
	block := make([]byte, length)
	if err := r.client.Download(r.ctx, r.file.Id, r.file.HeadRevision, off, length, func(body io.Reader) error {
		_, err := io.ReadFull(body, block)
		return err
	}); err != nil {
		return r.readError(err)
	}
	r.block, r.blockStart = block, off
	return nil
}

// readError returns the error of reading, as the context's, when it's
// cancelled, or as the filer's of a request
func (r *driveReader) readError(err error) error {
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return r.apiError("read", r.path, err)
}

// close cancels the request, and waits for it to stop
func (s *driveStream) close() {
	s.cancel()
	s.r.Close()
	<-s.done
}
