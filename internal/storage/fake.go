package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type fakeFile struct {
	data      []byte
	visibleAt time.Time
}

type FakeBackend struct {
	mu sync.Mutex

	files     map[string][]fakeFile
	puts      map[string]int
	uploads   map[string]int
	downloads map[string]int
	deletes   map[string]int

	uploadFailures   int
	listFailures     int
	downloadFailures int
	deleteFailures   int
	visibilityDelay  time.Duration
	duplicateList    map[string]int
	reverseList      bool
	quotaFailures    map[string]int
}

func NewFakeBackend() *FakeBackend {
	return &FakeBackend{
		files:         make(map[string][]fakeFile),
		puts:          make(map[string]int),
		uploads:       make(map[string]int),
		downloads:     make(map[string]int),
		deletes:       make(map[string]int),
		duplicateList: make(map[string]int),
		quotaFailures: make(map[string]int),
	}
}

func (b *FakeBackend) Login(ctx context.Context) error {
	return nil
}

func (b *FakeBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.fail("upload"); err != nil {
		return err
	}
	for _, file := range b.files[filename] {
		if time.Now().After(file.visibleAt) {
			return nil
		}
	}
	payload, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	b.uploads[filename]++
	b.files[filename] = append(b.files[filename], fakeFile{
		data:      payload,
		visibleAt: time.Now().Add(b.visibilityDelay),
	})
	return nil
}

func (b *FakeBackend) Put(ctx context.Context, filename string, data io.Reader) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.fail("put"); err != nil {
		return err
	}
	payload, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	b.puts[filename]++
	b.files[filename] = []fakeFile{{
		data:      payload,
		visibleAt: time.Now().Add(b.visibilityDelay),
	}}
	return nil
}

func (b *FakeBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.fail("list"); err != nil {
		return nil, err
	}
	now := time.Now()
	var names []string
	for name, entries := range b.files {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		for _, entry := range entries {
			if now.Before(entry.visibleAt) {
				continue
			}
			names = append(names, name)
			for i := 0; i < b.duplicateList[name]; i++ {
				names = append(names, name)
			}
			break
		}
	}
	sort.Strings(names)
	if b.reverseList {
		for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
			names[i], names[j] = names[j], names[i]
		}
	}
	return names, nil
}

func (b *FakeBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.downloads[filename]++
	if err := b.fail("download"); err != nil {
		return nil, err
	}
	entries := b.files[filename]
	if len(entries) == 0 {
		return nil, fmt.Errorf("file not found: %s", filename)
	}
	for _, entry := range entries {
		if time.Now().Before(entry.visibleAt) {
			continue
		}
		return io.NopCloser(bytes.NewReader(entry.data)), nil
	}
	return nil, fmt.Errorf("file not visible: %s", filename)
}

func (b *FakeBackend) Delete(ctx context.Context, filename string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.fail("delete"); err != nil {
		return err
	}
	b.deletes[filename]++
	delete(b.files, filename)
	return nil
}

func (b *FakeBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	return "fake-folder-" + name, nil
}

func (b *FakeBackend) FindFolder(ctx context.Context, name string) (string, error) {
	return "", nil
}

func (b *FakeBackend) SetUploadFailures(n int)   { b.mu.Lock(); b.uploadFailures = n; b.mu.Unlock() }
func (b *FakeBackend) SetListFailures(n int)     { b.mu.Lock(); b.listFailures = n; b.mu.Unlock() }
func (b *FakeBackend) SetDownloadFailures(n int) { b.mu.Lock(); b.downloadFailures = n; b.mu.Unlock() }
func (b *FakeBackend) SetDeleteFailures(n int)   { b.mu.Lock(); b.deleteFailures = n; b.mu.Unlock() }
func (b *FakeBackend) SetVisibilityDelay(d time.Duration) {
	b.mu.Lock()
	b.visibilityDelay = d
	b.mu.Unlock()
}

func (b *FakeBackend) SetDuplicateListing(filename string, duplicates int) {
	b.mu.Lock()
	b.duplicateList[filename] = duplicates
	b.mu.Unlock()
}

func (b *FakeBackend) SetReverseList(reverse bool) {
	b.mu.Lock()
	b.reverseList = reverse
	b.mu.Unlock()
}

func (b *FakeBackend) SetQuotaFailures(op string, count int) {
	b.mu.Lock()
	b.quotaFailures[op] = count
	b.mu.Unlock()
}

func (b *FakeBackend) HasFile(filename string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.files[filename]
	return ok
}

func (b *FakeBackend) PutCalls(filename string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts[filename]
}

func (b *FakeBackend) UploadCalls(filename string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.uploads[filename]
}

func (b *FakeBackend) DeleteCalls(filename string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deletes[filename]
}

func (b *FakeBackend) DownloadCalls(filename string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.downloads[filename]
}

func (b *FakeBackend) fail(op string) error {
	if count := b.quotaFailures[op]; count > 0 {
		b.quotaFailures[op] = count - 1
		return &httpStatusError{Operation: op, StatusCode: 429, Body: "quota exceeded"}
	}
	switch op {
	case "upload":
		if b.uploadFailures > 0 {
			b.uploadFailures--
			return fmt.Errorf("forced upload failure")
		}
	case "list":
		if b.listFailures > 0 {
			b.listFailures--
			return fmt.Errorf("forced list failure")
		}
	case "download":
		if b.downloadFailures > 0 {
			b.downloadFailures--
			return fmt.Errorf("forced download failure")
		}
	case "delete":
		if b.deleteFailures > 0 {
			b.deleteFailures--
			return fmt.Errorf("forced delete failure")
		}
	case "put":
		if b.uploadFailures > 0 {
			b.uploadFailures--
			return fmt.Errorf("forced put failure")
		}
	}
	return nil
}
