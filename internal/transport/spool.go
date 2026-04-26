package transport

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	segmentFileMagic  = "FDQ1"
	segmentBatchMagic = "FDQ2"
	compressionOff    = byte(0)
	compressionGzip   = byte(1)
)

type AckState struct {
	ClientID      string    `json:"client_id"`
	SessionID     string    `json:"session_id"`
	Direction     Direction `json:"direction"`
	AckedSeq      uint64    `json:"acked_seq"`
	UpdatedUnixMs int64     `json:"updated_unix_ms"`
}

type ackStoreRecord struct {
	ClientID           string    `json:"client_id"`
	SessionID          string    `json:"session_id"`
	Direction          Direction `json:"direction"`
	AckedSeq           uint64    `json:"acked_seq"`
	UpdatedUnixMs      int64     `json:"updated_unix_ms"`
	LastUploadedUnixMs int64     `json:"last_uploaded_unix_ms,omitempty"`
}

func newAckStoreRecord(ack AckState, lastUploadedUnixMs int64) ackStoreRecord {
	return ackStoreRecord{
		ClientID:           ack.ClientID,
		SessionID:          ack.SessionID,
		Direction:          ack.Direction,
		AckedSeq:           ack.AckedSeq,
		UpdatedUnixMs:      ack.UpdatedUnixMs,
		LastUploadedUnixMs: lastUploadedUnixMs,
	}
}

func (r ackStoreRecord) AckState() AckState {
	return AckState{
		ClientID:      r.ClientID,
		SessionID:     r.SessionID,
		Direction:     r.Direction,
		AckedSeq:      r.AckedSeq,
		UpdatedUnixMs: r.UpdatedUnixMs,
	}
}

type spoolSegmentMeta struct {
	BackendName      string    `json:"backend_name"`
	Direction        Direction `json:"direction"`
	ClientID         string    `json:"client_id"`
	SessionID        string    `json:"session_id"`
	Seq              uint64    `json:"seq"`
	EndSeq           uint64    `json:"end_seq,omitempty"`
	RemoteName       string    `json:"remote_name"`
	Abandoned        bool      `json:"abandoned,omitempty"`
	Uploaded         bool      `json:"uploaded"`
	UploadAttempts   int       `json:"upload_attempts"`
	NextUploadUnixMs int64     `json:"next_upload_unix_ms"`
	DeletedRemote    bool      `json:"deleted_remote"`
	DeleteAttempts   int       `json:"delete_attempts"`
	NextDeleteUnixMs int64     `json:"next_delete_unix_ms"`
	CreatedUnixMs    int64     `json:"created_unix_ms"`
	UpdatedUnixMs    int64     `json:"updated_unix_ms"`
	Compression      string    `json:"compression"`
}

type spoolSegment struct {
	Meta     spoolSegmentMeta
	DataPath string
	MetaPath string
}

func (s *spoolSegment) loadData() ([]byte, error) {
	return os.ReadFile(s.DataPath)
}

func (s *spoolSegment) saveMeta() error {
	s.Meta.UpdatedUnixMs = time.Now().UnixMilli()
	data, err := json.MarshalIndent(&s.Meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.MetaPath, data, 0644)
}

type spoolStore struct {
	baseDir string
	mu      sync.Mutex
}

func newSpoolStore(baseDir string) *spoolStore {
	return &spoolStore{baseDir: baseDir}
}

func (s *spoolStore) ensure() error {
	return os.MkdirAll(s.baseDir, 0755)
}

func (s *spoolStore) segmentPaths(backendName, remoteName string) (string, string) {
	dir := filepath.Join(s.baseDir, "segments", backendName)
	base := filepath.Join(dir, remoteName)
	return base + ".bin", base + ".json"
}

func (s *spoolStore) ackPath(backendName, filename string) string {
	return filepath.Join(s.baseDir, "acks", backendName, filename)
}

func (s *spoolStore) SaveSegment(meta spoolSegmentMeta, payload []byte) (*spoolSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensure(); err != nil {
		return nil, err
	}
	dataPath, metaPath := s.segmentPaths(meta.BackendName, meta.RemoteName)
	if err := os.MkdirAll(filepath.Dir(dataPath), 0755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(dataPath, payload, 0644); err != nil {
		return nil, err
	}
	meta.CreatedUnixMs = time.Now().UnixMilli()
	meta.UpdatedUnixMs = meta.CreatedUnixMs
	seg := &spoolSegment{
		Meta:     meta,
		DataPath: dataPath,
		MetaPath: metaPath,
	}
	if err := seg.saveMeta(); err != nil {
		return nil, err
	}
	return seg, nil
}

func (s *spoolStore) DeleteSegment(seg *spoolSegment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(seg.DataPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(seg.MetaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *spoolStore) LoadSegments() ([]*spoolSegment, error) {
	var segments []*spoolSegment
	root := filepath.Join(s.baseDir, "segments")
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		metaBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var meta spoolSegmentMeta
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return err
		}
		dataPath := strings.TrimSuffix(path, ".json") + ".bin"
		segments = append(segments, &spoolSegment{
			Meta:     meta,
			DataPath: dataPath,
			MetaPath: path,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].Meta.BackendName != segments[j].Meta.BackendName {
			return segments[i].Meta.BackendName < segments[j].Meta.BackendName
		}
		if segments[i].Meta.SessionID != segments[j].Meta.SessionID {
			return segments[i].Meta.SessionID < segments[j].Meta.SessionID
		}
		return segments[i].Meta.Seq < segments[j].Meta.Seq
	})
	return segments, nil
}

func (s *spoolStore) SaveAck(backendName, filename string, ack AckState) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.ackPath(backendName, filename)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", err
	}
	lastUploadedUnixMs := int64(0)
	if existing, err := os.ReadFile(path); err == nil {
		record, err := decodeAckStoreRecord(existing)
		if err != nil {
			return "", err
		}
		lastUploadedUnixMs = record.LastUploadedUnixMs
	}
	record := newAckStoreRecord(ack, lastUploadedUnixMs)
	payload, err := json.MarshalIndent(&record, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, payload, 0644); err != nil {
		return "", err
	}
	return path, nil
}

func (s *spoolStore) MarkAckUploaded(backendName, filename string, uploadedUnixMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.ackPath(backendName, filename)
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	record, err := decodeAckStoreRecord(payload)
	if err != nil {
		return err
	}
	record.LastUploadedUnixMs = uploadedUnixMs

	encoded, err := json.MarshalIndent(&record, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0644)
}

func (s *spoolStore) LoadAcks() (map[string]map[string]ackStoreRecord, error) {
	root := filepath.Join(s.baseDir, "acks")
	result := make(map[string]map[string]ackStoreRecord)
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		backendName := filepath.Base(filepath.Dir(path))
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		record, err := decodeAckStoreRecord(payload)
		if err != nil {
			return err
		}
		if result[backendName] == nil {
			result[backendName] = make(map[string]ackStoreRecord)
		}
		result[backendName][filepath.Base(path)] = record
		return nil
	})
	return result, err
}

func (s *spoolStore) DeleteAck(backendName, filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.ackPath(backendName, filename)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func decodeAckStoreRecord(payload []byte) (ackStoreRecord, error) {
	var record ackStoreRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return ackStoreRecord{}, err
	}
	return record, nil
}

func segmentFilename(dir Direction, clientID, sessionID string, seq uint64) string {
	return fmt.Sprintf("%s-%s-%s-%020d.bin", dir, safeID(clientID), safeID(sessionID), seq)
}

func segmentBatchFilename(dir Direction, clientID, sessionID string, startSeq, endSeq uint64) string {
	if startSeq == endSeq {
		return segmentFilename(dir, clientID, sessionID, startSeq)
	}
	return fmt.Sprintf("%s-%s-%s-%020d-%020d.bin", dir, safeID(clientID), safeID(sessionID), startSeq, endSeq)
}

func muxFilename(dir Direction, clientID string, tsUnixNano int64) string {
	return fmt.Sprintf("%s-%s-mux-%d.bin", dir, safeID(clientID), tsUnixNano)
}

func ackFilename(dir Direction, clientID, sessionID string) string {
	return fmt.Sprintf("ack-%s-%s-%s.json", dir, safeID(clientID), safeID(sessionID))
}

func parseMuxFilename(name string) (Direction, string, int64, bool) {
	if !strings.HasSuffix(name, ".bin") {
		return "", "", 0, false
	}
	base := strings.TrimSuffix(name, ".bin")
	parts := strings.Split(base, "-")
	if len(parts) != 4 || parts[2] != "mux" {
		return "", "", 0, false
	}
	clientID, err := unsafeID(parts[1])
	if err != nil {
		return "", "", 0, false
	}
	ts, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return "", "", 0, false
	}
	return Direction(parts[0]), clientID, ts, true
}

func parseSegmentFilename(name string) (Direction, string, string, uint64, uint64, bool) {
	if !strings.HasSuffix(name, ".bin") {
		return "", "", "", 0, 0, false
	}
	base := strings.TrimSuffix(name, ".bin")
	parts := strings.Split(base, "-")
	if len(parts) < 4 {
		return "", "", "", 0, 0, false
	}

	endSeqIdx := len(parts) - 1
	startSeqIdx := endSeqIdx
	startSeq, err := strconv.ParseUint(parts[startSeqIdx], 10, 64)
	if err != nil {
		return "", "", "", 0, 0, false
	}
	endSeq := startSeq
	if len(parts) >= 5 {
		if parsedStart, startErr := strconv.ParseUint(parts[endSeqIdx-1], 10, 64); startErr == nil {
			if parsedEnd, endErr := strconv.ParseUint(parts[endSeqIdx], 10, 64); endErr == nil {
				startSeq = parsedStart
				endSeq = parsedEnd
				startSeqIdx = endSeqIdx - 1
			}
		}
	}
	clientID, err := unsafeID(parts[1])
	if err != nil {
		return "", "", "", 0, 0, false
	}
	sessionID, err := unsafeID(strings.Join(parts[2:startSeqIdx], "-"))
	if err != nil {
		return "", "", "", 0, 0, false
	}
	return Direction(parts[0]), clientID, sessionID, startSeq, endSeq, true
}

func parseAckFilename(name string) (Direction, string, string, bool) {
	if !strings.HasSuffix(name, ".json") {
		return "", "", "", false
	}
	base := strings.TrimSuffix(name, ".json")
	parts := strings.Split(base, "-")
	if len(parts) < 4 || parts[0] != "ack" {
		return "", "", "", false
	}
	clientID, err := unsafeID(parts[2])
	if err != nil {
		return "", "", "", false
	}
	sessionID, err := unsafeID(strings.Join(parts[3:], "-"))
	if err != nil {
		return "", "", "", false
	}
	return Direction(parts[1]), clientID, sessionID, true
}

func marshalSegmentPayload(env *Envelope, compression string, compressionMinBytes int) ([]byte, string, error) {
	return marshalSegmentPayloads([]*Envelope{env}, compression, compressionMinBytes)
}

func marshalSegmentPayloads(envs []*Envelope, compression string, compressionMinBytes int) ([]byte, string, error) {
	if len(envs) == 0 {
		return nil, "", fmt.Errorf("no envelopes to marshal")
	}

	magic := segmentFileMagic
	var raw []byte
	if len(envs) == 1 {
		encoded, err := envs[0].MarshalBinary()
		if err != nil {
			return nil, "", err
		}
		raw = encoded
	} else {
		magic = segmentBatchMagic
		var batch bytes.Buffer
		var countBuf [2]byte
		binary.BigEndian.PutUint16(countBuf[:], uint16(len(envs)))
		batch.Write(countBuf[:])
		for _, env := range envs {
			encoded, err := env.MarshalBinary()
			if err != nil {
				return nil, "", err
			}
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(encoded)))
			batch.Write(lenBuf[:])
			batch.Write(encoded)
		}
		raw = batch.Bytes()
	}

	applied := "off"
	body := raw
	if compression == "gzip" && len(raw) >= compressionMinBytes {
		var compressed bytes.Buffer
		gz := gzip.NewWriter(&compressed)
		if _, err := gz.Write(raw); err != nil {
			return nil, "", err
		}
		if err := gz.Close(); err != nil {
			return nil, "", err
		}
		body = compressed.Bytes()
		applied = "gzip"
	} else if compression != "" && compression != "off" && compression != "gzip" {
		return nil, "", fmt.Errorf("unsupported compression mode: %s", compression)
	}

	var buf bytes.Buffer
	buf.WriteString(magic)
	switch applied {
	case "off":
		buf.WriteByte(compressionOff)
	case "gzip":
		buf.WriteByte(compressionGzip)
	default:
		return nil, "", fmt.Errorf("unsupported applied compression: %s", applied)
	}
	buf.Write(body)
	return buf.Bytes(), applied, nil
}

func unmarshalSegmentPayload(payload []byte) (*Envelope, error) {
	envs, err := unmarshalSegmentPayloads(payload)
	if err != nil {
		return nil, err
	}
	if len(envs) != 1 {
		return nil, fmt.Errorf("expected single envelope payload, got %d", len(envs))
	}
	return envs[0], nil
}

func unmarshalSegmentPayloads(payload []byte) ([]*Envelope, error) {
	if len(payload) < len(segmentFileMagic)+1 {
		return nil, fmt.Errorf("invalid segment payload header")
	}

	magic := string(payload[:len(segmentFileMagic)])
	if magic != segmentFileMagic && magic != segmentBatchMagic {
		return nil, fmt.Errorf("invalid segment payload header")
	}
	compression := payload[len(segmentFileMagic)]
	body := payload[len(segmentFileMagic)+1:]
	switch compression {
	case compressionOff:
	case compressionGzip:
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		decoded, err := io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return nil, err
		}
		body = decoded
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", compression)
	}

	if magic == segmentFileMagic {
		env := &Envelope{}
		if err := env.Decode(bytes.NewReader(body)); err != nil {
			return nil, err
		}
		return []*Envelope{env}, nil
	}

	reader := bytes.NewReader(body)
	var countBuf [2]byte
	if _, err := io.ReadFull(reader, countBuf[:]); err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint16(countBuf[:]))
	if count <= 0 {
		return nil, fmt.Errorf("invalid envelope batch count: %d", count)
	}

	envs := make([]*Envelope, 0, count)
	for i := 0; i < count; i++ {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint32(lenBuf[:])
		if size == 0 || size > 32*1024*1024 {
			return nil, fmt.Errorf("invalid envelope size: %d", size)
		}
		encoded := make([]byte, size)
		if _, err := io.ReadFull(reader, encoded); err != nil {
			return nil, err
		}
		env := &Envelope{}
		if err := env.Decode(bytes.NewReader(encoded)); err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	return envs, nil
}

func safeID(v string) string {
	return hex.EncodeToString([]byte(v))
}

func unsafeID(v string) (string, error) {
	decoded, err := hex.DecodeString(v)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}
