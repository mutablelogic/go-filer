package enrichment

import (
	// The handlers of go-metadata, of each of its kinds of file, which keep
	// those which need cgo, and its C libraries, behind its build tags:
	// audio and video, of FFmpeg 8, photos' EXIF, HEIF and RAW, of libexif,
	// libheif and LibRaw, and classifying audio, of libfvad, with the libfvad
	// tag, and fingerprinting it, of Chromaprint, with the chromaprint tag.
	// Without cgo, the others are built: documents, email, text, images,
	// such as PNG, JPEG, SVG and Photoshop, playlists, subtitles, the kinds
	// of audio and video, and the movies and episodes on TMDB, and the
	// recordings on AcoustID, by their tags, which are looked up with
	// WithTMDB, and WithAcoustID.
	_ "github.com/mutablelogic/go-metadata/application"
	_ "github.com/mutablelogic/go-metadata/audio"
	_ "github.com/mutablelogic/go-metadata/image"
	_ "github.com/mutablelogic/go-metadata/message"
	_ "github.com/mutablelogic/go-metadata/text"
	_ "github.com/mutablelogic/go-metadata/video"
)
