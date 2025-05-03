package rss_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/rss"
	"github.com/stretchr/testify/assert"
)

func Test_Parser_001(t *testing.T) {
	assert := assert.New(t)
	files, err := filepath.Glob("rsstest/*.rss")
	assert.Nil(err)
	assert.NotNil(files)
	assert.NotEmpty(files)
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			fh, err := os.Open(file)
			assert.NoError(err)
			assert.NotNil(fh)
			defer fh.Close()
			feed, err := rss.Read(fh)
			assert.NoError(err)
			assert.NotNil(feed)
			if feed != nil {
				if !compare(assert, feed, file+".json") {
					t.Log(feed)
				}
			}
		})
	}
}

func Test_Parser_002(t *testing.T) {
	assert := assert.New(t)
	files, err := filepath.Glob("itunestest/*.rss")
	assert.Nil(err)
	assert.NotNil(files)
	assert.NotEmpty(files)
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			fh, err := os.Open(file)
			assert.NoError(err)
			assert.NotNil(fh)
			defer fh.Close()
			feed, err := rss.Read(fh)
			assert.NoError(err)
			assert.NotNil(feed)
			if !compare(assert, feed, file+".json") {
				t.Log(feed)
			}
		})
	}
}

func Test_Parser_003(t *testing.T) {
	assert := assert.New(t)
	files, err := filepath.Glob("datetest/*.rss")
	assert.Nil(err)
	assert.NotNil(files)
	assert.NotEmpty(files)
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			fh, err := os.Open(file)
			assert.NoError(err)
			assert.NotNil(fh)
			defer fh.Close()
			feed, err := rss.Read(fh)
			assert.NoError(err)
			assert.NotNil(feed)

			// Parse date values
			for _, item := range feed.Channel.Items {
				if item.PubDate != nil {
					_, err := item.PubDate.Parse()
					assert.NoError(err)
				}
			}
		})
	}
}

func Test_Parser_004(t *testing.T) {
	assert := assert.New(t)
	files, err := filepath.Glob("durationtest/*.rss")
	assert.Nil(err)
	assert.NotNil(files)
	assert.NotEmpty(files)
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			fh, err := os.Open(file)
			assert.NoError(err)
			assert.NotNil(fh)
			defer fh.Close()
			feed, err := rss.Read(fh)
			assert.NoError(err)
			assert.NotNil(feed)

			if feed.Channel.TTL != nil {
				_, err := feed.Channel.TTL.Minutes()
				assert.NoError(err)
			}

			for _, item := range feed.Channel.Items {
				if item.Duration != nil {
					dur, err := item.Duration.Seconds()
					assert.NoError(err)
					t.Log(item.Duration, "=>", dur)
				}
			}

		})
	}
}

///////////////////////////////////////////////////////////////////////////////
// Private methods

func compare(a *assert.Assertions, feed *rss.Feed, file string) bool {
	result, err := os.ReadFile(file)
	a.NoError(err)
	a.NotNil(result)
	a.Equal(strings.TrimSpace(string(result)), feed.String())
	return strings.TrimSpace(string(result)) == feed.String()
}
