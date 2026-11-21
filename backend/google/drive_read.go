package google

import (
	"context"
	"errors"
	"io"
	"io/fs"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	blocks "github.com/mutablelogic/go-filer/backend/internal/blocks"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// driveReader reads a file's content, of its revision when it was listed,
// from its offset, which can seek, and be read at an offset, so a file,
// such as a video, is read in place, of the ranges which are read, rather
// than all of it. What's read is requested from the offset, and is read as
// it's received, until it's sought elsewhere, when it's requested again,
// from there. ReadAt requests blocks of the file, apart from Read and
// Seek, the last of which are kept, so a parser's reads, which are
// scattered across a file, such as a PDF's objects, are of few requests.
type driveReader struct {
	*DriveFiler
	ctx    context.Context
	path   string // The file's path, of its errors
	file   *httpclient.DriveFile
	size   int64
	offset int64          // Where the next read is from
	stream *driveStream   // What's requested, from where it's read, or nil
	blocks *blocks.Reader // The blocks which ReadAt requests, and keeps
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
// file, or the offset, and requests it from there, unless it's requested
// from there, or it's at the end of the file, or beyond it, so a request
// which fails is known when it seeks, before a response of the range is
// written
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
	if r.offset < r.size && (r.stream == nil || r.stream.at != r.offset) {
		if err := r.open(r.offset); err != nil {
			return 0, err
		}
	}
	return r.offset, nil
}

// ReadAt reads the content at the offset, of the blocks which are
// requested of their ranges, the last of which are kept, apart from Read
// and Seek, and is safe to call at once
func (r *driveReader) ReadAt(p []byte, off int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	} else if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: r.path, Err: fs.ErrInvalid}
	}
	return r.blocks.ReadAt(p, off)
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

// newDriveReader returns a reader of the file, of its size, which isn't
// requested until it's read
func newDriveReader(ctx context.Context, filer *DriveFiler, path string, file *httpclient.DriveFile, size int64) *driveReader {
	r := &driveReader{DriveFiler: filer, ctx: ctx, path: path, file: file, size: size}
	r.blocks = blocks.New(size, r.fetch)
	return r
}

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

// fetch requests the content of the file at the offset, of the buffer's
// length, which is within the file
func (r *driveReader) fetch(p []byte, off int64) error {
	if err := r.client.Download(r.ctx, r.file.Id, r.file.HeadRevision, off, int64(len(p)), func(body io.Reader) error {
		_, err := io.ReadFull(body, p)
		return err
	}); err != nil {
		return r.readError(err)
	}
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
