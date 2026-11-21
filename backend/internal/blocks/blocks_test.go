package blocks_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	// Packages
	blocks "github.com/mutablelogic/go-filer/backend/internal/blocks"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// file returns the content of a file of the size, and a reader of it, whose
// requests are counted, and fail with the error, when it's set
func file(size int) ([]byte, *blocks.Reader, *[][2]int64, *error) {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 7)
	}
	var requests [][2]int64
	var fail error
	r := blocks.New(int64(size), func(p []byte, off int64) error {
		requests = append(requests, [2]int64{off, int64(len(p))})
		if fail != nil {
			return fail
		}
		copy(p, data[off:])
		return nil
	})
	return data, r, &requests, &fail
}

func Test_Reader(t *testing.T) {
	const size = 40*blocks.Size + 1000
	data, r, requests, fail := file(size)
	read := func(off int64, n int) ([]byte, error) {
		p := make([]byte, n)
		got, err := r.ReadAt(p, off)
		return p[:got], err
	}

	// A read is of its block, from its start
	p, err := read(100, 10)
	require.NoError(t, err)
	assert.Equal(t, data[100:110], p)
	assert.Equal(t, [][2]int64{{0, blocks.Size}}, *requests)

	// Reads which are scattered across blocks which are kept, such as a
	// parser's of the end of a file, and its objects, aren't requested again
	for range 100 {
		for _, off := range []int64{size - 50, 200, 3*blocks.Size + 5, 10 * blocks.Size} {
			p, err := read(off, 40)
			require.NoError(t, err)
			assert.Equal(t, data[off:off+40], p)
		}
	}
	assert.Len(t, *requests, 4)

	// A read which spans blocks is one request, of them
	*requests = nil
	p, err = read(20*blocks.Size-10, blocks.Size+20)
	require.NoError(t, err)
	assert.Equal(t, data[20*blocks.Size-10:21*blocks.Size+10], p)
	assert.Equal(t, [][2]int64{{19 * blocks.Size, 3 * blocks.Size}}, *requests)

	// The blocks read least recently are requested again, once more than
	// Blocks are read
	*requests = nil
	for i := range blocks.Blocks {
		_, err := read(int64(25+i)*blocks.Size, 1)
		require.NoError(t, err)
	}
	assert.Len(t, *requests, blocks.Blocks)
	_, err = read(int64(25+blocks.Blocks-1)*blocks.Size, 1)
	require.NoError(t, err)
	assert.Len(t, *requests, blocks.Blocks, "the last is kept")
	_, err = read(100, 1)
	require.NoError(t, err)
	assert.Len(t, *requests, blocks.Blocks+1, "the first isn't")

	// The end of the file is its last block, which is short, and a read
	// beyond it is io.EOF
	_, fresh, requests, _ := file(size)
	p = make([]byte, 20)
	got, err := fresh.ReadAt(p, size-10)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, data[size-10:], p[:got])
	assert.Equal(t, [][2]int64{{40 * blocks.Size, 1000}}, *requests)
	_, err = read(size, 1)
	assert.Equal(t, io.EOF, err)

	// A request which fails is the error, and isn't kept
	*fail = errors.New("unavailable")
	_, err = read(5*blocks.Size, 1)
	assert.Equal(t, *fail, err)
	*fail = nil
	p, err = read(5*blocks.Size, 4)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data[5*blocks.Size:5*blocks.Size+4], p))
}
