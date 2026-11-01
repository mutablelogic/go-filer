// Package seaweedfs watches the changes to the objects of an S3 bucket of a
// SeaweedFS filer, by subscribing to the changes to the filer's metadata,
// with its gRPC service
package seaweedfs

// The client of the filer's gRPC service, which is generated with protoc,
// and its plugins for Go and gRPC (see "make protobuf-generate")
//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative filer_pb/filer.proto
