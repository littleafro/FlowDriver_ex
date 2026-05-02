package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestIsRateLimitedError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "429", err: &httpStatusError{Operation: "put", StatusCode: 429, Body: "too many requests"}, want: true},
		{name: "403 quota", err: &httpStatusError{Operation: "list", StatusCode: 403, Body: "user rate limit exceeded"}, want: true},
		{name: "local budget", err: errRateBudgetExhausted, want: true},
		{name: "401", err: &httpStatusError{Operation: "token", StatusCode: 401, Body: "invalid credentials"}, want: false},
		{name: "generic", err: io.EOF, want: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isRateLimitedError(tc.err); got != tc.want {
				t.Fatalf("isRateLimitedError(%v)=%v want %v", tc.err, got, tc.want)
			}
		})
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

func TestValidateFolderAcceptsAccessibleDriveFolder(t *testing.T) {
	t.Parallel()

	backend := &GoogleBackend{
		apiClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/drive/v3/files/folder-123" {
				t.Fatalf("unexpected path %q", r.URL.Path)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
				t.Fatalf("Authorization = %q, want Bearer test-token", got)
			}
			if got := r.URL.Query().Get("supportsAllDrives"); got != "true" {
				t.Fatalf("supportsAllDrives = %q, want true", got)
			}
			return jsonResponse(http.StatusOK, `{"id":"folder-123","name":"Flow-Data","mimeType":"application/vnd.google-apps.folder","trashed":false}`), nil
		})},
		tokenClient:     http.DefaultClient,
		credentialsPath: "/tmp/credentials.json",
		folderID:        "folder-123",
		token:           "test-token",
		tokenEx:         time.Now().Add(time.Hour),
	}

	if err := backend.ValidateFolder(context.Background()); err != nil {
		t.Fatalf("ValidateFolder failed: %v", err)
	}
}

func TestValidateFolderReturnsReauthHintForInaccessibleConfiguredFolder(t *testing.T) {
	t.Parallel()

	backend := &GoogleBackend{
		apiClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusNotFound, `{"error":{"message":"File not found"}}`), nil
		})},
		tokenClient:     http.DefaultClient,
		credentialsPath: "/tmp/credentials.json",
		folderID:        "folder-123",
		token:           "test-token",
		tokenEx:         time.Now().Add(time.Hour),
	}

	err := backend.ValidateFolder(context.Background())
	if err == nil {
		t.Fatalf("ValidateFolder unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "folder_id \"folder-123\" is not accessible") {
		t.Fatalf("expected inaccessible-folder hint, got %v", err)
	}
	if !strings.Contains(err.Error(), "/tmp/credentials.json.token") {
		t.Fatalf("expected token cache hint, got %v", err)
	}
}

func TestLocalBackendConcurrentPut(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	backend, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend failed: %v", err)
	}

	const writers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(strings.Repeat("x", 1024+i))
			if err := backend.Put(context.Background(), "manifest-req-test.json", bytes.NewReader(payload)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Put failed: %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") || strings.Contains(entry.Name(), ".tmp-") {
			t.Fatalf("unexpected temp file left behind: %s", filepath.Join(dir, entry.Name()))
		}
	}

	rc, err := backend.Download(context.Background(), "manifest-req-test.json")
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(body) == 0 {
		t.Fatalf("expected final file to be non-empty")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
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
