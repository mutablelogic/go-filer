package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_transfer(t *testing.T) {
	// Without a terminal, there's no progress, and the reader is the file
	progress := newTransfer(0, 100)
	assert.Nil(t, progress)
	r := io.NopCloser(strings.NewReader("abc"))
	assert.Equal(t, r, progress.Reader("a.txt", r))
	progress.Close()

	// With a terminal, the bytes read are counted, and drawn, with the file
	// which is being read, and cleared once it's closed
	progress = newTransfer(80, 6)
	require.NotNil(t, progress)
	var buf bytes.Buffer
	progress.w = &buf
	for _, file := range []string{"a.txt", "b.txt"} {
		data, err := io.ReadAll(progress.Reader(file, io.NopCloser(strings.NewReader("abc"))))
		require.NoError(t, err)
		assert.Equal(t, "abc", string(data))
	}
	assert.Equal(t, int64(6), progress.done.Load())
	progress.Close()
	progress.draw()
	assert.Contains(t, buf.String(), "6 B of 6 B b.txt")
	assert.Contains(t, buf.String(), "100.0%")
}
