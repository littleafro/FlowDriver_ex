package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

// streamManager handles a single appendable stream file per direction per client.
// Client appends envelope bytes to local stream file, uploads entire file each flush.
// Server downloads the stream file and reads from its last known offset.
type streamManager struct {
	baseDir string
	mu      sync.Mutex

	streams map[Direction]*streamState
}

type streamState struct {
	dataPath string
	offset   int64
}

func newStreamManager(baseDir string) *streamManager {
	return &streamManager{
		baseDir: baseDir,
		streams: make(map[Direction]*streamState),
	}
}

func (m *streamManager) getOrCreate(d Direction, clientID string) *streamState {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.streams[d]
	if !ok {
		dir := filepath.Join(m.baseDir, "streams", string(d))
		_ = os.MkdirAll(dir, 0755)
		s = &streamState{
			dataPath: filepath.Join(dir, "stream.bin"),
			offset:   0,
		}
		m.streams[d] = s
	}
	return s
}

func streamFile(d Direction, clientID string) string {
	return "stream-" + string(d) + "-" + safeID(clientID) + ".bin"
}

// Append writes raw envelope bytes to the local stream file.
// Returns the starting offset of the appended data.
func (m *streamManager) Append(d Direction, clientID string, data []byte) (int64, error) {
	s := m.getOrCreate(d, clientID)

	f, err := os.OpenFile(s.dataPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}
	pos, err := f.Seek(0, 2)
	if err != nil {
		f.Close()
		return 0, err
	}
	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return 0, err
	}
	f.Close()

	s.offset = pos + int64(len(data))
	return pos, nil
}

// Flush uploads the entire stream file to the backend.
// Returns the total uploaded file size, or 0 if no data.
func (m *streamManager) Flush(d Direction, clientID string, backend storage.Backend, ctx context.Context) (int64, error) {
	s := m.getOrCreate(d, clientID)

	data, err := os.ReadFile(s.dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}

	filename := streamFile(d, clientID)
	if err := backend.Put(ctx, filename, bytes.NewReader(data)); err != nil {
		return 0, err
	}

	s.offset = int64(len(data))
	return int64(len(data)), nil
}

// DownloadStream downloads the entire stream file from the backend.
func (m *streamManager) DownloadStream(d Direction, clientID string, backend storage.Backend, ctx context.Context) ([]byte, error) {
	filename := streamFile(d, clientID)

	rc, err := backend.Download(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// streamOffset tracks the server's read offset for a stream, persisted to Drive.
type streamOffset struct {
	Dir            Direction `json:"dir"`
	ClientID       string    `json:"client_id"`
	Offset         int64     `json:"offset"`
	LastUploadedMs int64     `json:"last_uploaded_ms"`
}

func saveOffsetToDrive(offset streamOffset, backend storage.Backend, ctx context.Context) error {
	filename := offsetFile(offset.Dir, offset.ClientID)
	data, err := json.MarshalIndent(offset, "", "  ")
	if err != nil {
		return err
	}
	return backend.Put(ctx, filename, bytes.NewReader(data))
}

func loadOffsetFromDrive(backend storage.Backend, d Direction, clientID string, ctx context.Context) (*streamOffset, error) {
	filename := offsetFile(d, clientID)

	rc, err := backend.Download(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var offset streamOffset
	if err := json.NewDecoder(rc).Decode(&offset); err != nil {
		return nil, err
	}
	return &offset, nil
}

// offsetFile returns the Drive filename for the offset file.
func offsetFile(d Direction, clientID string) string {
	return "offset-" + string(d) + "-" + safeID(clientID) + ".json"
}

// lastUploadTime returns how long ago the last offset was uploaded.
func (o *streamOffset) lastUploadTime() time.Duration {
	if o.LastUploadedMs == 0 {
		return time.Hour
	}
	return time.Since(time.UnixMilli(o.LastUploadedMs))
}
