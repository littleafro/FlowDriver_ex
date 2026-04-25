package storage

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

func rendezvousScore(sessionKey, backendName string, weight int) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionKey))
	_, _ = h.Write([]byte("::"))
	_, _ = h.Write([]byte(backendName))
	sum := h.Sum64()

	// Convert to a stable (0,1] value for weighted rendezvous hashing.
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sum)
	u := float64(binary.BigEndian.Uint64(buf[:])>>11) / float64(uint64(1)<<53)
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	if u >= 1 {
		u = math.Nextafter(1, 0)
	}
	return -math.Log(u) / float64(weight)
}
