package mime

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type namedBuffer struct {
	*bytes.Reader
	name string
}

func (r namedBuffer) Name() string {
	return r.name
}

func TestTypeTSByContent(t *testing.T) {
	t.Run("mpeg transport stream", func(t *testing.T) {
		data := make([]byte, 188*3)
		data[0] = 0x47
		data[188] = 0x47
		data[376] = 0x47

		contentType, _, err := Type(namedBuffer{Reader: bytes.NewReader(data), name: "movie.ts"})
		require.NoError(t, err)
		require.Equal(t, "video/mp2t", contentType)
	})

	t.Run("typescript", func(t *testing.T) {
		contentType, _, err := Type(namedBuffer{Reader: bytes.NewReader([]byte("export const value: string = 'ok'\n")), name: "index.ts"})
		require.NoError(t, err)
		require.Equal(t, "text/typescript", contentType)
	})
}
