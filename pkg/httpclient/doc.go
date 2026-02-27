// Package httpclient provides a typed Go client for consuming the filer
// REST API.
//
// Create a client with:
//
//	client, err := httpclient.New("http://localhost:8080/api/filer")
//	if err != nil {
//	   panic(err)
//	}
//
// Then use the client to manage files:
//
//	// List all backends
//	backends, err := client.ListBackends(ctx)
package httpclient
