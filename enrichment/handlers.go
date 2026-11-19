package enrichment

import (
	// The handlers of go-metadata which are pure Go: documents, email,
	// text, images, such as PNG, JPEG and SVG, playlists, subtitles, the
	// kinds of audio and video, and the movies and episodes on TMDB which
	// videos are, with WithTMDB. Those of audio, video, and photos' EXIF,
	// HEIF and RAW, need cgo (see handlers_cgo.go), and Photoshop's is pure Go
	// without it (see handlers_nocgo.go).
	_ "github.com/mutablelogic/go-metadata/application"
	_ "github.com/mutablelogic/go-metadata/audio/kind"
	_ "github.com/mutablelogic/go-metadata/audio/playlist"
	_ "github.com/mutablelogic/go-metadata/image/raster"
	_ "github.com/mutablelogic/go-metadata/image/svg"
	_ "github.com/mutablelogic/go-metadata/message"
	_ "github.com/mutablelogic/go-metadata/text"
	_ "github.com/mutablelogic/go-metadata/video/kind"
	_ "github.com/mutablelogic/go-metadata/video/subtitle"
	_ "github.com/mutablelogic/go-metadata/video/tmdb"
)
