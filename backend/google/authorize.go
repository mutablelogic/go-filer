package google

import (
	"context"

	// Packages
	auth "github.com/mutablelogic/go-auth/auth/httpclient"
)

func (c *Client) DiscoverConfig(ctx context.Context) (*auth.Config, error) {
	return c.Discover(context.Background(), "https://accounts.google.com")
}
