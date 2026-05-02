package transport

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	chunkFileMagic      = "FDC1"
	chunkBatchMagic     = "FDC2"
	chunkCompressionOff = byte(0)
	chunkCompressionGzip = byte(1)
)

type manifestChunk struct {
	ID            string `json:"id"`
	CreatedUnixMs int64  `json:"created_unix_ms"`
	SizeBytes     int    `json:"size_bytes"`
}

type manifestFile struct {
	Dir           Direction       `json:"dir"`
	ClientID      string          `json:"client_id"`
	Epoch         uint64          `json:"epoch"`
	UpdatedUnixMs int64           `json:"updated_unix_ms"`
	Chunks        []manifestChunk `json:"chunks"`
}

type sessionHeadFile struct {
	Dir           Direction       `json:"dir"`
	ClientID      string          `json:"client_id"`
	SessionID     string          `json:"session_id"`
	TargetAddr    string          `json:"target_addr,omitempty"`
	Epoch         uint64          `json:"epoch"`
	UpdatedUnixMs int64           `json:"updated_unix_ms"`
	Chunks        []manifestChunk `json:"chunks"`
}

func manifestFilename(d Direction, clientID string) string {
	return "manifest-" + string(d) + "-" + safeID(clientID) + ".json"
}

func parseManifestFilename(name string) (Direction, string, bool) {
	if !strings.HasSuffix(name, ".json") {
		return "", "", false
	}
	base := strings.TrimSuffix(name, ".json")
	parts := strings.Split(base, "-")
	if len(parts) != 3 || parts[0] != "manifest" {
		return "", "", false
	}
	clientID, err := unsafeID(parts[2])
	if err != nil {
		return "", "", false
	}
	return Direction(parts[1]), clientID, true
}

func chunkFilename(d Direction, clientID string, epoch uint64, chunkID string) string {
	return fmt.Sprintf("chunk-%s-%s-%d-%s.bin", string(d), safeID(clientID), epoch, chunkID)
}

func sessionHeadFilename(d Direction, sessionID string) string {
	return fmt.Sprintf("sess-%s-%s-head.json", safeID(sessionID), string(d))
}

func sessionChunkFilename(d Direction, sessionID string, epoch uint64, chunkID string) string {
	return fmt.Sprintf("sess-%s-%s-%d-%s.bin", safeID(sessionID), string(d), epoch, chunkID)
}

func parseChunkFilename(name string) (Direction, string, uint64, string, int64, bool) {
	if !strings.HasSuffix(name, ".bin") {
		return "", "", 0, "", 0, false
	}
	base := strings.TrimSuffix(name, ".bin")
	parts := strings.Split(base, "-")
	if len(parts) < 5 || parts[0] != "chunk" {
		return "", "", 0, "", 0, false
	}
	clientID, err := unsafeID(parts[2])
	if err != nil {
		return "", "", 0, "", 0, false
	}
	epoch, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return "", "", 0, "", 0, false
	}
	chunkID := strings.Join(parts[4:], "-")
	createdUnixMs := parseChunkCreatedUnixMs(chunkID)
	return Direction(parts[1]), clientID, epoch, chunkID, createdUnixMs, true
}

func newChunkID(now time.Time) string {
	var randBytes [4]byte
	_, _ = rand.Read(randBytes[:])
	return fmt.Sprintf("%013d-%08x", now.UnixMilli(), randBytes)
}

func parseChunkCreatedUnixMs(chunkID string) int64 {
	prefix, _, ok := strings.Cut(chunkID, "-")
	if !ok {
		return 0
	}
	createdUnixMs, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0
	}
	return createdUnixMs
}

func marshalChunkPayloads(envs []*Envelope, compression string, compressionMinBytes int) ([]byte, string, error) {
	if len(envs) == 0 {
		return nil, "", fmt.Errorf("no envelopes to marshal")
	}

	magic := chunkFileMagic
	var raw []byte
	if len(envs) == 1 {
		encoded, err := envs[0].MarshalBinary()
		if err != nil {
			return nil, "", err
		}
		raw = encoded
	} else {
		magic = chunkBatchMagic
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
		buf.WriteByte(chunkCompressionOff)
	case "gzip":
		buf.WriteByte(chunkCompressionGzip)
	default:
		return nil, "", fmt.Errorf("unsupported applied compression: %s", applied)
	}
	buf.Write(body)
	return buf.Bytes(), applied, nil
}

func unmarshalChunkPayloads(payload []byte) ([]*Envelope, error) {
	if len(payload) < len(chunkFileMagic)+1 {
		return nil, fmt.Errorf("invalid chunk payload header")
	}

	magic := string(payload[:len(chunkFileMagic)])
	if magic != chunkFileMagic && magic != chunkBatchMagic {
		return nil, fmt.Errorf("invalid chunk payload header")
	}
	compression := payload[len(chunkFileMagic)]
	body := payload[len(chunkFileMagic)+1:]
	switch compression {
	case chunkCompressionOff:
	case chunkCompressionGzip:
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		decoded, err := io.ReadAll(gr)
		_ = gr.Close()
		if err != nil {
			return nil, err
		}
		body = decoded
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", compression)
	}

	if magic == chunkFileMagic {
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

func encodeSessionHead(head sessionHeadFile) ([]byte, error) {
	head.UpdatedUnixMs = time.Now().UnixMilli()
	return json.MarshalIndent(head, "", "  ")
}

func decodeSessionHead(payload []byte) (*sessionHeadFile, error) {
	var head sessionHeadFile
	if err := json.Unmarshal(payload, &head); err != nil {
		return nil, err
	}
	return &head, nil
}

type seenChunkCache struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	order []string
	items map[string]time.Time
}

func newSeenChunkCache(maxEntries int, ttl time.Duration) *seenChunkCache {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return &seenChunkCache{
		max:   maxEntries,
		ttl:   ttl,
		order: make([]string, 0, maxEntries),
		items: make(map[string]time.Time, maxEntries),
	}
}

func (c *seenChunkCache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(time.Now())
	_, ok := c.items[id]
	return ok
}

func (c *seenChunkCache) Mark(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.pruneLocked(now)
	if _, exists := c.items[id]; exists {
		c.items[id] = now
		return
	}
	c.items[id] = now
	c.order = append(c.order, id)
	for len(c.order) > c.max {
		evict := c.order[0]
		c.order = c.order[1:]
		delete(c.items, evict)
	}
}

func (c *seenChunkCache) Reset() {
	c.mu.Lock()
	c.order = c.order[:0]
	c.items = make(map[string]time.Time, c.max)
	c.mu.Unlock()
}

func (c *seenChunkCache) pruneLocked(now time.Time) {
	if c.ttl <= 0 || len(c.order) == 0 {
		return
	}
	keep := c.order[:0]
	for _, id := range c.order {
		seenAt, ok := c.items[id]
		if !ok {
			continue
		}
		if now.Sub(seenAt) > c.ttl {
			delete(c.items, id)
			continue
		}
		keep = append(keep, id)
	}
	c.order = keep
}

func encodeManifest(m manifestFile) ([]byte, error) {
	m.UpdatedUnixMs = time.Now().UnixMilli()
	return json.MarshalIndent(m, "", "  ")
}

func decodeManifest(data []byte) (*manifestFile, error) {
	var m manifestFile
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
