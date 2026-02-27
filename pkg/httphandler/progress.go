package httphandler

import "io"

///////////////////////////////////////////////////////////////////////////////
// CONSTANTS

// progressChunk is the number of bytes transferred between successive
// UploadFileEvent emissions during a streaming upload.
const progressChunk int64 = 64 * 1024 // 64 KiB

///////////////////////////////////////////////////////////////////////////////
// TYPES

// progressReader wraps an io.Reader and calls emit after every progressChunk
// bytes have been read. It does not emit on EOF — the caller is expected to
// emit a final UploadCompleteEvent once CreateObject returns.
type progressReader struct {
	r       io.Reader
	written int64
	emitted int64
	total   int64 // declared part size, 0 if unknown
	emit    func(written, total int64)
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newProgressReader(r io.Reader, total int64, emit func(written, total int64)) *progressReader {
	return &progressReader{r: r, total: total, emit: emit}
}

///////////////////////////////////////////////////////////////////////////////
// io.Reader

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.written += int64(n)
		for p.written-p.emitted >= progressChunk {
			p.emitted += progressChunk
			p.emit(p.emitted, p.total)
		}
	}
	return n, err
}
