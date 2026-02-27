package backend

import (
	"net/url"
	"testing"

	// Packages
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithEndpoint(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		wantErr   bool
		wantQuery map[string]string
	}{
		{
			name:     "http endpoint",
			endpoint: "http://localhost:9000",
			wantErr:  false,
			wantQuery: map[string]string{
				"endpoint":         "http://localhost:9000",
				"s3ForcePathStyle": "true",
				"disable_https":    "true",
			},
		},
		{
			name:     "https endpoint",
			endpoint: "https://s3.example.com",
			wantErr:  false,
			wantQuery: map[string]string{
				"endpoint":         "https://s3.example.com",
				"s3ForcePathStyle": "true",
			},
		},
		{
			name:     "http endpoint with port",
			endpoint: "http://192.168.1.1:8333",
			wantErr:  false,
			wantQuery: map[string]string{
				"endpoint":         "http://192.168.1.1:8333",
				"s3ForcePathStyle": "true",
				"disable_https":    "true",
			},
		},
		{
			name:     "invalid scheme",
			endpoint: "ftp://example.com",
			wantErr:  true,
		},
		{
			name:     "invalid URL",
			endpoint: "://invalid",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			u, err := url.Parse("s3://mybucket")
			require.NoError(err)

			o, err := apply(u, WithEndpoint(tt.endpoint))

			if tt.wantErr {
				assert.Error(err)
				return
			}

			require.NoError(err)

			for key, want := range tt.wantQuery {
				assert.Equal(want, o.url.Query().Get(key), "query param %q", key)
			}

			// Ensure disable_https is not set for https
			if tt.endpoint[:5] == "https" {
				assert.Empty(o.url.Query().Get("disable_https"), "disable_https should not be set for https")
			}
		})
	}
}

func TestWithRegion(t *testing.T) {
	tests := []struct {
		name   string
		region string
		want   string
	}{
		{"us-east-1", "us-east-1", "us-east-1"},
		{"eu-west-1", "eu-west-1", "eu-west-1"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			u, err := url.Parse("s3://mybucket")
			require.NoError(err)

			o, err := apply(u, WithRegion(tt.region))
			require.NoError(err)

			assert.Equal(tt.want, o.url.Query().Get("region"))
		})
	}
}

func TestWithProfile(t *testing.T) {
	tests := []struct {
		name    string
		profile string
		want    string
	}{
		{"default", "default", "default"},
		{"custom", "my-profile", "my-profile"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			u, err := url.Parse("s3://mybucket")
			require.NoError(err)

			o, err := apply(u, WithProfile(tt.profile))
			require.NoError(err)

			assert.Equal(tt.want, o.url.Query().Get("profile"))
		})
	}
}

func TestCombinedOptions(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	u, err := url.Parse("s3://mybucket")
	require.NoError(err)

	o, err := apply(u,
		WithEndpoint("http://localhost:9000"),
		WithRegion("us-east-1"),
		WithProfile("myprofile"),
	)
	require.NoError(err)

	q := o.url.Query()

	expected := map[string]string{
		"endpoint":         "http://localhost:9000",
		"s3ForcePathStyle": "true",
		"disable_https":    "true",
		"region":           "us-east-1",
		"profile":          "myprofile",
	}

	for key, want := range expected {
		assert.Equal(want, q.Get(key), "query param %q", key)
	}
}

func TestApplyWithNilURL(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	o, err := apply(nil, WithRegion("us-east-1"))
	require.NoError(err)
	assert.Nil(o.url)
}
