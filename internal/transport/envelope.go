package transport

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type EnvelopeKind uint8

const (
	KindOpen EnvelopeKind = iota + 1
	KindData
	KindClose
	KindHeartbeat
)

const (
	MagicByte      = 0x1F
	EnvelopeV2Byte = 0x02
)

// Envelope is the durable unit exchanged through the storage backend.
type Envelope struct {
	SessionID     string
	Seq           uint64
	Kind          EnvelopeKind
	TargetAddr    string
	Payload       []byte
	CreatedUnixMs int64
	Checksum      uint32
}

func (k EnvelopeKind) String() string {
	switch k {
	case KindOpen:
		return "open"
	case KindData:
		return "data"
	case KindClose:
		return "close"
	case KindHeartbeat:
		return "heartbeat"
	default:
		return "unknown"
	}
}

func (e *Envelope) computeChecksum() uint32 {
	table := crc32.NewIEEE()
	table.Write([]byte(e.SessionID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], e.Seq)
	table.Write(buf[:])
	buf = [8]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(e.CreatedUnixMs))
	table.Write(buf[:])
	table.Write([]byte{byte(e.Kind)})
	table.Write([]byte(e.TargetAddr))
	table.Write(e.Payload)
	return table.Sum32()
}

func (e *Envelope) EnsureChecksum() {
	e.Checksum = e.computeChecksum()
}

func (e *Envelope) VerifyChecksum() bool {
	return e.Checksum == e.computeChecksum()
}

func (e *Envelope) MarshalBinary() ([]byte, error) {
	if e.Kind == 0 {
		e.Kind = KindData
	}
	if e.CreatedUnixMs == 0 {
		return nil, fmt.Errorf("created_unix_ms must be set")
	}
	if e.Checksum == 0 {
		e.EnsureChecksum()
	}

	sessionBytes := []byte(e.SessionID)
	targetBytes := []byte(e.TargetAddr)
	total := 2 + 1 + 2 + len(sessionBytes) + 8 + 8 + 2 + len(targetBytes) + 4 + 4 + len(e.Payload)
	buf := make([]byte, total)
	buf[0] = MagicByte
	buf[1] = EnvelopeV2Byte
	buf[2] = byte(e.Kind)
	binary.BigEndian.PutUint16(buf[3:], uint16(len(sessionBytes)))
	offset := 5
	copy(buf[offset:], sessionBytes)
	offset += len(sessionBytes)
	binary.BigEndian.PutUint64(buf[offset:], e.Seq)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.CreatedUnixMs))
	offset += 8
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(targetBytes)))
	offset += 2
	copy(buf[offset:], targetBytes)
	offset += len(targetBytes)
	binary.BigEndian.PutUint32(buf[offset:], e.Checksum)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Payload)))
	offset += 4
	copy(buf[offset:], e.Payload)
	return buf, nil
}

func (e *Envelope) Encode(w io.Writer) error {
	data, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (e *Envelope) Decode(r io.Reader) error {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	if hdr[0] != MagicByte {
		return fmt.Errorf("invalid magic byte: 0x%X", hdr[0])
	}
	if hdr[1] != EnvelopeV2Byte {
		return fmt.Errorf("unsupported envelope version: %d", hdr[1])
	}

	e.Kind = EnvelopeKind(hdr[2])
	sessionLen := int(binary.BigEndian.Uint16(hdr[3:5]))
	sessionBytes := make([]byte, sessionLen)
	if _, err := io.ReadFull(r, sessionBytes); err != nil {
		return err
	}
	e.SessionID = string(sessionBytes)

	var seqBuf [8]byte
	if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
		return err
	}
	e.Seq = binary.BigEndian.Uint64(seqBuf[:])

	var createdBuf [8]byte
	if _, err := io.ReadFull(r, createdBuf[:]); err != nil {
		return err
	}
	e.CreatedUnixMs = int64(binary.BigEndian.Uint64(createdBuf[:]))

	var targetLenBuf [2]byte
	if _, err := io.ReadFull(r, targetLenBuf[:]); err != nil {
		return err
	}
	targetLen := int(binary.BigEndian.Uint16(targetLenBuf[:]))
	targetBytes := make([]byte, targetLen)
	if _, err := io.ReadFull(r, targetBytes); err != nil {
		return err
	}
	e.TargetAddr = string(targetBytes)

	var checksumBuf [4]byte
	if _, err := io.ReadFull(r, checksumBuf[:]); err != nil {
		return err
	}
	e.Checksum = binary.BigEndian.Uint32(checksumBuf[:])

	var payloadLenBuf [4]byte
	if _, err := io.ReadFull(r, payloadLenBuf[:]); err != nil {
		return err
	}
	payloadLen := binary.BigEndian.Uint32(payloadLenBuf[:])
	if payloadLen > 32*1024*1024 {
		return fmt.Errorf("payload too large: %d", payloadLen)
	}
	if payloadLen > 0 {
		e.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, e.Payload); err != nil {
			return err
		}
	} else {
		e.Payload = nil
	}

	if !e.VerifyChecksum() {
		return fmt.Errorf("checksum mismatch for session %s seq %d", e.SessionID, e.Seq)
	}
	return nil
}
