package storage

import (
	"context"
	"io"
)

// Backend defines the interface for our pluggable storage mechanism that acts as the
// covert transport layer.
type Backend interface {
	// Login performs any necessary authentication.
	Login(ctx context.Context) error

	// Upload stores a file if it does not already exist. The operation must be
	// safe to retry with the same filename.
	Upload(ctx context.Context, filename string, data io.Reader) error

	// Put stores or replaces a file by name.
	Put(ctx context.Context, filename string, data io.Reader) error

	// ListQuery searches the backend for files matching a specific prefix or criteria.
	// We use this to discover new request or response payloads.
	ListQuery(ctx context.Context, prefix string) ([]string, error)

	// Download returns an io.ReadCloser for the file content from the backend.
	Download(ctx context.Context, filename string) (io.ReadCloser, error)

	// Delete removes a file from the backend after it has been read or expired.
	Delete(ctx context.Context, filename string) error

	// CreateFolder creates a storage container (e.g. Google Drive folder) and returns its ID.
	CreateFolder(ctx context.Context, name string) (string, error)

	// FindFolder searches for an existing storage container by name and returns its ID.
	FindFolder(ctx context.Context, name string) (string, error)
}
