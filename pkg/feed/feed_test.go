package feed_test

import (
	"context"
	"testing"

	// Packages

	test "github.com/djthorpe/go-pg/pkg/test"
	feed "github.com/mutablelogic/go-filer/pkg/feed"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

// Global connection variable
var conn test.Conn

// Start up a container and test the pool
func TestMain(m *testing.M) {
	test.Main(m, &conn)
}

/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func Test_Manager_001(t *testing.T) {
	assertouter := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	// Create a new feed manager
	manager, err := feed.NewManager(context.TODO(), conn, nil)
	if !assertouter.NoError(err) {
		t.FailNow()
	}
	assertouter.NotNil(manager)

	t.Run("InsertUrl", func(t *testing.T) {
		assert := assert.New(t)
		url, err := manager.CreateUrl(context.TODO(), schema.UrlMeta{
			Url: types.StringPtr("https://example.com"),
		})
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.NotNil(url)
	})

	t.Run("ListUrl", func(t *testing.T) {
		assert := assert.New(t)
		list, err := manager.ListUrls(context.TODO(), schema.UrlListRequest{})
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.NotNil(list)
	})

	t.Run("GetUrl1", func(t *testing.T) {
		assert := assert.New(t)
		_, err := manager.GetUrl(context.TODO(), 9999999999)
		assert.ErrorIs(err, httpresponse.ErrNotFound)
	})

	t.Run("GetUrl2", func(t *testing.T) {
		assert := assert.New(t)
		url, err := manager.CreateUrl(context.TODO(), schema.UrlMeta{
			Url: types.StringPtr("https://example2.com"),
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		url2, err := manager.GetUrl(context.TODO(), url.Id)
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.NotNil(url2)
		assert.Equal(url, url2)
	})

	t.Run("DeleteUrl1", func(t *testing.T) {
		assert := assert.New(t)
		_, err := manager.DeleteUrl(context.TODO(), 9999999999)
		assert.ErrorIs(err, httpresponse.ErrNotFound)
	})

	t.Run("DeleteUrl2", func(t *testing.T) {
		assert := assert.New(t)
		url, err := manager.CreateUrl(context.TODO(), schema.UrlMeta{
			Url: types.StringPtr("https://example3.com"),
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		url2, err := manager.DeleteUrl(context.TODO(), url.Id)
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.NotNil(url2)
		assert.Equal(url, url2)

		_, err = manager.GetUrl(context.TODO(), url.Id)
		assert.ErrorIs(err, httpresponse.ErrNotFound)
	})

}
