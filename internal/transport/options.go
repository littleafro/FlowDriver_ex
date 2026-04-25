package transport

import "time"

type Options struct {
	DataDir                      string
	ClientID                     string
	ServerID                     string
	TunnelID                     string
	SessionIdleTimeout           time.Duration
	StaleUnackedFileTTL          time.Duration
	TombstoneTTL                 time.Duration
	CleanupInterval              time.Duration
	HeartbeatInterval            time.Duration
	PollRate                     time.Duration
	ActivePollRate               time.Duration
	IdlePollRate                 time.Duration
	FlushRate                    time.Duration
	SegmentBytes                 int
	MaxSegmentBytes              int
	MaxMuxSegments               int
	MaxConcurrentUploads         int
	MaxConcurrentDownloads       int
	MaxConcurrentDeletes         int
	MaxPendingSegmentsPerSession int
	MaxTxBufferBytesPerSession   int
	MaxOutOfOrderSegments        int
	AckEverySegments             int
	AckInterval                  time.Duration
	Compression                  string
	CompressionMinBytes          int
}

func DefaultOptions() Options {
	return Options{
		DataDir:                      "./data",
		TunnelID:                     "main",
		SessionIdleTimeout:           5 * time.Minute,
		StaleUnackedFileTTL:          30 * time.Minute,
		TombstoneTTL:                 5 * time.Minute,
		CleanupInterval:              30 * time.Second,
		HeartbeatInterval:            30 * time.Second,
		PollRate:                     750 * time.Millisecond,
		ActivePollRate:               500 * time.Millisecond,
		IdlePollRate:                 3 * time.Second,
		FlushRate:                    500 * time.Millisecond,
		SegmentBytes:                 64 * 1024,
		MaxSegmentBytes:              256 * 1024,
		MaxMuxSegments:               32,
		MaxConcurrentUploads:         4,
		MaxConcurrentDownloads:       4,
		MaxConcurrentDeletes:         2,
		MaxPendingSegmentsPerSession: 256,
		MaxTxBufferBytesPerSession:   4 * 1024 * 1024,
		MaxOutOfOrderSegments:        64,
		AckEverySegments:             8,
		AckInterval:                  time.Second,
		Compression:                  "off",
		CompressionMinBytes:          4096,
	}
}

func (o *Options) ApplyDefaults() {
	def := DefaultOptions()
	if o.DataDir == "" {
		o.DataDir = def.DataDir
	}
	if o.TunnelID == "" {
		o.TunnelID = def.TunnelID
	}
	if o.SessionIdleTimeout <= 0 {
		o.SessionIdleTimeout = def.SessionIdleTimeout
	}
	if o.StaleUnackedFileTTL <= 0 {
		o.StaleUnackedFileTTL = def.StaleUnackedFileTTL
	}
	if o.TombstoneTTL <= 0 {
		o.TombstoneTTL = def.TombstoneTTL
	}
	if o.CleanupInterval <= 0 {
		o.CleanupInterval = def.CleanupInterval
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = def.HeartbeatInterval
	}
	if o.PollRate <= 0 {
		o.PollRate = def.PollRate
	}
	if o.ActivePollRate <= 0 {
		o.ActivePollRate = def.ActivePollRate
	}
	if o.IdlePollRate <= 0 {
		o.IdlePollRate = def.IdlePollRate
	}
	if o.FlushRate <= 0 {
		o.FlushRate = def.FlushRate
	}
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = def.SegmentBytes
	}
	if o.MaxSegmentBytes <= 0 {
		o.MaxSegmentBytes = def.MaxSegmentBytes
	}
	if o.MaxMuxSegments <= 0 {
		o.MaxMuxSegments = def.MaxMuxSegments
	}
	if o.MaxConcurrentUploads <= 0 {
		o.MaxConcurrentUploads = def.MaxConcurrentUploads
	}
	if o.MaxConcurrentDownloads <= 0 {
		o.MaxConcurrentDownloads = def.MaxConcurrentDownloads
	}
	if o.MaxConcurrentDeletes <= 0 {
		o.MaxConcurrentDeletes = def.MaxConcurrentDeletes
	}
	if o.MaxPendingSegmentsPerSession <= 0 {
		o.MaxPendingSegmentsPerSession = def.MaxPendingSegmentsPerSession
	}
	if o.MaxTxBufferBytesPerSession <= 0 {
		o.MaxTxBufferBytesPerSession = def.MaxTxBufferBytesPerSession
	}
	if o.MaxOutOfOrderSegments <= 0 {
		o.MaxOutOfOrderSegments = def.MaxOutOfOrderSegments
	}
	if o.AckEverySegments <= 0 {
		o.AckEverySegments = def.AckEverySegments
	}
	if o.AckInterval <= 0 {
		o.AckInterval = def.AckInterval
	}
	if o.Compression == "" {
		o.Compression = def.Compression
	}
	if o.CompressionMinBytes <= 0 {
		o.CompressionMinBytes = def.CompressionMinBytes
	}
}
