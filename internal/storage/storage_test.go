package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"testing"
	"time"
)

func TestRetryableHTTPStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  error
		want bool
	}{
		{err: &httpStatusError{Operation: "upload", StatusCode: 429, Body: "rate limited"}, want: true},
		{err: &httpStatusError{Operation: "list", StatusCode: 500, Body: "internal error"}, want: true},
		{err: &httpStatusError{Operation: "list", StatusCode: 503, Body: "unavailable"}, want: true},
		{err: &httpStatusError{Operation: "list", StatusCode: 403, Body: "user rate limit exceeded"}, want: true},
		{err: &httpStatusError{Operation: "token", StatusCode: 401, Body: "unauthorized"}, want: false},
	}

	for _, tc := range cases {
		if got := isRetryableError(tc.err); got != tc.want {
			t.Fatalf("isRetryableError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestRateLimiterWaits(t *testing.T) {
	t.Parallel()

	limiter := newRequestRateLimiter("test", RateLimitConfig{
		MaxReadsPerSecond: 5,
	})
	start := time.Now()
	for i := 0; i < 6; i++ {
		if err := limiter.wait(context.Background(), "download"); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("expected limiter to block, got elapsed=%s", elapsed)
	}
}

func TestBackendSelectionConsistent(t *testing.T) {
	t.Parallel()

	pool := NewBackendPool(
		&BackendHandle{Name: "g1", Weight: 1, Backend: NewFakeBackend(), health: HealthHealthy},
		&BackendHandle{Name: "g2", Weight: 1, Backend: NewFakeBackend(), health: HealthHealthy},
	)

	first, err := pool.Select("session-1")
	if err != nil {
		t.Fatalf("first select failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		next, err := pool.Select("session-1")
		if err != nil {
			t.Fatalf("select %d failed: %v", i, err)
		}
		if next.Name != first.Name {
			t.Fatalf("selection not consistent: got %s, want %s", next.Name, first.Name)
		}
	}
}

func TestMultipartBodyOmitsParentsForUpdate(t *testing.T) {
	t.Parallel()

	backend := &GoogleBackend{folderID: "folder-123"}

	createContentType, createBody, err := backend.multipartBody("ack.json", []byte("abc"), true)
	if err != nil {
		t.Fatalf("create multipartBody failed: %v", err)
	}
	createMeta := decodeMultipartMeta(t, createContentType, createBody)
	if _, ok := createMeta["parents"]; !ok {
		t.Fatalf("expected create metadata to include parents")
	}

	updateContentType, updateBody, err := backend.multipartBody("ack.json", []byte("abc"), false)
	if err != nil {
		t.Fatalf("update multipartBody failed: %v", err)
	}
	updateMeta := decodeMultipartMeta(t, updateContentType, updateBody)
	if _, ok := updateMeta["parents"]; ok {
		t.Fatalf("expected update metadata to omit parents, got %v", updateMeta["parents"])
	}
}

func decodeMultipartMeta(t *testing.T, contentType string, body []byte) map[string]any {
	t.Helper()

	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("ParseMediaType failed: %v", err)
	}

	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	part, err := reader.NextPart()
	if err != nil {
		t.Fatalf("NextPart failed: %v", err)
	}
	defer part.Close()

	metaBytes, err := io.ReadAll(part)
	if err != nil {
		t.Fatalf("ReadAll meta failed: %v", err)
	}

	var meta map[string]any
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("Unmarshal meta failed: %v", err)
	}
	return meta
}
