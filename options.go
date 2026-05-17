package memcache

import (
	"time"

	"github.com/ubgo/cache"
)

// Weigher returns the cost of a value for size-aware (max-bytes) eviction.
// Default counts raw value bytes.
type Weigher func(val []byte) int64

// config is the resolved option set. Built by defaults() then mutated by the
// applied Option funcs in New. Zero-valued caps/intervals mean the
// corresponding feature is disabled (unbounded / lazy-only / no persistence).
type config struct {
	shards        int
	maxEntries    int64 // global; 0 = unlimited
	maxBytes      int64 // global; 0 = unlimited
	weigher       Weigher
	onEvict       func(key string, cause cache.EvictionCause)
	clock         func() time.Time
	sweepInterval time.Duration // 0 = lazy expiry only
	policy        Policy
	ckptPath      string
	ckptInterval  time.Duration
	aofPath       string
}

// Option configures New.
type Option func(*config)

// WithShards sets the shard count (default 16). Rounded up to a power of two.
func WithShards(n int) Option { return func(c *config) { c.shards = n } }

// WithMaxEntries caps total entries; least-recently-used are evicted past it.
func WithMaxEntries(n int64) Option { return func(c *config) { c.maxEntries = n } }

// WithMaxBytes caps total weighed size; LRU eviction enforces it.
func WithMaxBytes(n int64) Option { return func(c *config) { c.maxBytes = n } }

// WithWeigher overrides the per-entry cost function used by WithMaxBytes.
func WithWeigher(w Weigher) Option { return func(c *config) { c.weigher = w } }

// WithOnEvict registers a callback fired for every eviction with its cause.
func WithOnEvict(fn func(key string, cause cache.EvictionCause)) Option {
	return func(c *config) { c.onEvict = fn }
}

// WithSweepInterval runs a background goroutine that proactively drops expired
// entries every d (in addition to lazy expiry on read). Stopped by Close.
func WithSweepInterval(d time.Duration) Option {
	return func(c *config) { c.sweepInterval = d }
}

// WithClock overrides the time source (tests only).
func WithClock(fn func() time.Time) Option { return func(c *config) { c.clock = fn } }

// WithCheckpoint periodically snapshots the cache to path every interval
// (atomic write via temp file + rename) and writes a final checkpoint on
// Close. Pair with RestoreFromFile at startup for crash-resilient warm state.
func WithCheckpoint(path string, interval time.Duration) Option {
	return func(c *config) { c.ckptPath = path; c.ckptInterval = interval }
}

// WithAOF enables append-only-file durability at path: every write
// (Set/SetMulti/SetNX/Expire/Incr/Decr/Del/Flush) is fsync-appended to the
// log, and an existing log is replayed into memory on New before appends
// resume. Near-zero-loss durability at the cost of a sync per write; call
// CompactAOF periodically to bound the log. Composes with WithCheckpoint.
func WithAOF(path string) Option { return func(c *config) { c.aofPath = path } }

// WithPolicy selects the eviction algorithm (default LRU). AdaptiveWTinyLFU
// trades a little memory for substantially higher hit rates on skewed and
// scan-heavy workloads.
func WithPolicy(p Policy) Option { return func(c *config) { c.policy = p } }

// defaults returns the baseline config. Note policy is explicitly set to
// AdaptiveWTinyLFU here even though LRU is the iota zero value: the default
// policy is W-TinyLFU (PLAN Q6 / §1.5), and relying on the zero value would
// silently make LRU the default instead.
func defaults() config {
	return config{
		shards:  16,
		weigher: func(v []byte) int64 { return int64(len(v)) },
		clock:   time.Now,
		policy:  AdaptiveWTinyLFU, // PLAN Q6 / §1.5: W-TinyLFU is the default
	}
}

// nextPow2 rounds n up to the next power of two (minimum 1). The shard count
// must be a power of two so shardFor's modulo collapses to an AND with the
// mask.
func nextPow2(n int) int {
	if n < 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
