package schema

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	// SSE event names emitted during a streaming upload (objectUploadSSEStream).
	// Clients should switch on these names to drive per-file progress UIs.

	// UploadStartEvent is sent once before the upload loop begins.
	// Payload: UploadStart
	UploadStartEvent = "start"

	// UploadFileEvent is sent each time the progress reader crosses a chunk
	// boundary (~64 KiB) while a file is being transferred. The first emission
	// for each file has Written == 0 and serves as the "file started" signal.
	// Payload: UploadFile
	UploadFileEvent = "file"

	// UploadCompleteEvent is sent after each file has been successfully
	// committed to the backend. Payload: schema.Object
	UploadCompleteEvent = "complete"

	// UploadErrorEvent is sent if any file fails. Rollback of previously
	// committed files is attempted before this event is emitted.
	// The stream is closed immediately after. Payload: UploadError
	UploadErrorEvent = "error"

	// UploadDoneEvent is sent after all files have been committed successfully,
	// just before the stream is closed. Payload: UploadDone
	UploadDoneEvent = "done"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// UploadStart is the payload for UploadStartEvent.
type UploadStart struct {
	// Files is the number of files to be uploaded.
	Files int `json:"files"`

	// Bytes is the sum of declared part sizes in bytes, or 0 if unknown
	// (multipart parts rarely include Content-Length).
	Bytes int64 `json:"bytes,omitempty"`
}

// UploadFile is the payload for UploadFileEvent.
type UploadFile struct {
	// Index is the 0-based position of the file in the upload.
	Index int `json:"index"`

	// Path is the destination object path (normalised).
	Path string `json:"path"`

	// Written is the number of bytes transferred so far for this file.
	Written int64 `json:"written"`

	// Bytes is the declared size of this file in bytes, or 0 if unknown
	// (multipart parts rarely include Content-Length).
	Bytes int64 `json:"bytes,omitempty"`
}

// UploadError is the payload for UploadErrorEvent.
type UploadError struct {
	// Index is the 0-based position of the file that failed.
	Index int `json:"index"`

	// Path is the destination object path that failed.
	Path string `json:"path"`

	// Message is the error description.
	Message string `json:"message"`
}

// UploadDone is the payload for UploadDoneEvent.
type UploadDone struct {
	// Files is the number of files successfully committed.
	Files int `json:"files"`

	// Bytes is the total number of bytes committed across all files.
	Bytes int64 `json:"bytes"`
}
