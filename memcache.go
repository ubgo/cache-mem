// memcache.go — the sharded in-memory cache.Cache adapter core (package memcache, github.com/ubgo/cache-mem).
//
// Package role: memcache is the in-memory adapter of the ubgo/cache
// family — a sharded, weight-aware store with lazy + background TTL,
// pluggable eviction, and optional snapshot/checkpoint/AOF durability.
// See doc.go for the full package overview and usage examples.
//
// This file: the Cache, shard, entry and iter types plus every
// cache.Cache method (Get/Set/Del/Incr/Iterate/Stats/Close, ...) and the
// internal store/getEntry/dropEntry/deleteEntry/trimBytes helpers. Keys
// map to one of N (power-of-two) shards via a per-cache seeded maphash so
// shardFor's modulo collapses to `& c.mask`; each shard has its own mutex
// and independent policy. New replays an existing AOF into memory (via
// the internal helpers, NOT public Set, so replay does not re-append)
// before opening the live append handle and starting the loops.
//
// AI-context: this is a decorator over plain maps — every public op locks
// exactly the owning shard (whole-cache ops lock shards serially, never
// all at once, so they can't deadlock); counters are atomics; the closed
// flag short-circuits every op to cache.ErrClosed and Close is idempotent.

package memcache

import (
	"context"
	"encoding/binary"
	"hash/maphash"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ubgo/cache"
)

// Cache is the sharded in-memory adapter. Construct with New and always Close
// it. The eviction policy (LRU or Adaptive W-TinyLFU) is selected via
// WithPolicy; the default is AdaptiveWTinyLFU.
//
// Concurrency model: every public operation acquires the owning shard's mutex
// (or, for whole-cache ops like Stats/Flush/snapshot, each shard mutex briefly
// and serially). Counters are atomics so Stats never needs a global lock.
// After Close every public op returns cache.ErrClosed; Close is idempotent.
type Cache struct {
	cfg    config
	mask   uint64       // == len(shards)-1; len(shards) is a power of two
	seed   maphash.Seed // per-cache random seed for shard placement
	shards []*shard

	stop   chan struct{} // closed by Close to stop sweeper/checkpoint loops
	closed atomic.Bool   // set once by Close; guards every public op

	hits, misses, sets, dels, evicts atomic.Int64
	evCauseMu                        sync.Mutex // guards evCause map only
	evCause                          map[cache.EvictionCause]int64
	aof                              *aofLog // nil unless WithAOF; nil-safe
}

// entry is one stored value. exp is an absolute deadline (zero = no expiry);
// it is computed from the relative TTL at store time and recomputed on
// restore so a restored entry never outlives its original deadline. weight is
// the cached cfg.weigher(val) result, kept so byte accounting on overwrite is
// a delta and never re-weighs.
type entry struct {
	val    []byte
	exp    time.Time // zero = no expiry; absolute deadline otherwise
	weight int64
}

// shard is one independently locked partition. Its policy (pol) tracks key
// ordering/frequency and chooses victims; pol is only ever called with mu
// held. bytes is the sum of live entry weights in this shard.
type shard struct {
	mu    sync.Mutex
	items map[string]*entry
	bytes int64
	pol   evictPolicy
}

// New builds an in-memory cache. Always Close it to stop the background
// sweeper / checkpoint goroutines and flush the AOF handle.
//
// Order of construction matters: shards (and their per-shard policy) are
// created first; then, if WithAOF was set, the existing log is replayed into
// memory WITHOUT re-logging (replay uses the internal store/delete helpers,
// not the public Set/Del, so it does not re-append) before the append handle
// is opened and live appends resume; only then are the sweeper and checkpoint
// loops started.
func New(opts ...Option) *Cache {
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.clock == nil {
		cfg.clock = time.Now
	}
	if cfg.weigher == nil {
		cfg.weigher = func(v []byte) int64 { return int64(len(v)) }
	}
	n := nextPow2(cfg.shards)
	c := &Cache{
		cfg:     cfg,
		mask:    uint64(n - 1),
		seed:    maphash.MakeSeed(),
		shards:  make([]*shard, n),
		stop:    make(chan struct{}),
		evCause: map[cache.EvictionCause]int64{},
	}
	capPerShard := 0
	if cfg.maxEntries > 0 {
		capPerShard = int(cfg.maxEntries) / n
		if capPerShard < 1 {
			capPerShard = 1
		}
	}
	for i := range c.shards {
		s := &shard{items: map[string]*entry{}}
		// drop removes a policy-displaced key from shard state. The policy has
		// already removed it from its own bookkeeping, so this must NOT call
		// back into the policy.
		drop := func(key string) { c.dropEntry(s, key, cache.EvictSize) }
		switch cfg.policy {
		case AdaptiveWTinyLFU:
			s.pol = newWTinyPolicy(capPerShard, drop)
		default:
			s.pol = newLRUPolicy(capPerShard, drop)
		}
		c.shards[i] = s
	}
	if cfg.aofPath != "" {
		// Replay existing log into memory (no re-logging), then resume appends.
		_ = replayAOF(cfg.aofPath, func(r aofRec) {
			switch r.Op {
			case aofSet:
				s := c.shardFor(r.Key)
				s.mu.Lock()
				c.store(s, r.Key, r.Val, r.TTL)
				s.mu.Unlock()
			case aofDel:
				s := c.shardFor(r.Key)
				s.mu.Lock()
				c.deleteEntry(s, r.Key, cache.EvictExplicit)
				s.mu.Unlock()
			case aofExpire:
				s := c.shardFor(r.Key)
				s.mu.Lock()
				if e, ok := c.getEntry(s, r.Key, c.now()); ok {
					if r.TTL > 0 {
						e.exp = c.now().Add(r.TTL)
					} else {
						e.exp = time.Time{}
					}
				}
				s.mu.Unlock()
			case aofFlush:
				for _, s := range c.shards {
					s.mu.Lock()
					s.items = map[string]*entry{}
					s.bytes = 0
					s.pol.clear()
					s.mu.Unlock()
				}
			}
		})
		if l, err := openAOF(cfg.aofPath); err == nil {
			c.aof = l
		}
	}
	if cfg.sweepInterval > 0 {
		go c.sweepLoop()
	}
	if cfg.ckptPath != "" && cfg.ckptInterval > 0 {
		go c.checkpointLoop()
	}
	return c
}

// shardFor maps a key to its shard. The shard count is a power of two so the
// modulo is a single AND with c.mask. The seed is per-cache and random, so
// placement is not stable across processes (intentional: shards are not
// externally observable, and a random seed defeats adversarial collision
// pinning).
func (c *Cache) shardFor(key string) *shard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	_, _ = h.WriteString(key)
	return c.shards[h.Sum64()&c.mask]
}

func (c *Cache) now() time.Time { return c.cfg.clock() }

func (c *Cache) maxBytesPerShard() int64 {
	if c.cfg.maxBytes <= 0 {
		return 0
	}
	per := c.cfg.maxBytes / int64(len(c.shards))
	if per < 1 {
		per = 1
	}
	return per
}

func (c *Cache) countEvict(cause cache.EvictionCause, key string) {
	c.evicts.Add(1)
	c.evCauseMu.Lock()
	c.evCause[cause]++
	c.evCauseMu.Unlock()
	if c.cfg.onEvict != nil {
		c.cfg.onEvict(key, cause)
	}
}

func expired(e *entry, now time.Time) bool {
	return !e.exp.IsZero() && !now.Before(e.exp)
}

// dropEntry removes a key from shard state only (map + bytes + eviction
// counter). It does not touch the policy. Caller holds the shard lock.
func (c *Cache) dropEntry(s *shard, key string, cause cache.EvictionCause) {
	if e, ok := s.items[key]; ok {
		delete(s.items, key)
		s.bytes -= e.weight
		c.countEvict(cause, key)
	}
}

// deleteEntry removes a key from both shard state and the policy. Caller holds
// the shard lock.
func (c *Cache) deleteEntry(s *shard, key string, cause cache.EvictionCause) {
	if _, ok := s.items[key]; ok {
		c.dropEntry(s, key, cause)
		s.pol.remove(key)
	}
}

// getEntry returns a live entry, lazily dropping it if expired, and records a
// policy hit. Caller holds the shard lock.
func (c *Cache) getEntry(s *shard, key string, now time.Time) (*entry, bool) {
	e, ok := s.items[key]
	if !ok {
		return nil, false
	}
	if expired(e, now) {
		c.deleteEntry(s, key, cache.EvictExpired)
		return nil, false
	}
	s.pol.hit(key)
	return e, true
}

// store inserts or overwrites. Caller holds the shard lock.
func (c *Cache) store(s *shard, key string, val []byte, ttl time.Duration) {
	now := c.now()
	var exp time.Time
	if ttl > 0 {
		exp = now.Add(ttl)
	}
	w := c.cfg.weigher(val)

	if e, ok := s.items[key]; ok {
		s.bytes += w - e.weight
		e.val, e.exp, e.weight = val, exp, w
		s.pol.hit(key)
		c.countEvict(cache.EvictReplaced, key)
		c.trimBytes(s)
		return
	}

	s.items[key] = &entry{val: val, exp: exp, weight: w}
	s.bytes += w
	if !s.pol.add(key) {
		// Admission denied: the policy's drop callback has already removed
		// this key from shard state. Nothing more to do.
		return
	}
	c.trimBytes(s)
}

// trimBytes evicts policy-chosen victims until the shard is within its byte
// cap. Caller holds the shard lock.
func (c *Cache) trimBytes(s *shard) {
	maxB := c.maxBytesPerShard()
	if maxB <= 0 {
		return
	}
	for s.bytes > maxB {
		k, ok := s.pol.evict()
		if !ok {
			return
		}
		c.dropEntry(s, k, cache.EvictSize)
	}
}

// Get implements cache.Cache.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.closed.Load() {
		return nil, cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := c.getEntry(s, key, c.now())
	if !ok {
		c.misses.Add(1)
		return nil, cache.ErrNotFound
	}
	c.hits.Add(1)
	out := make([]byte, len(e.val))
	copy(out, e.val)
	return out, nil
}

// GetMulti implements cache.Cache.
func (c *Cache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	if c.closed.Load() {
		return nil, cache.ErrClosed
	}
	out := make(map[string][]byte, len(keys))
	now := c.now()
	for _, k := range keys {
		s := c.shardFor(k)
		s.mu.Lock()
		if e, ok := c.getEntry(s, k, now); ok {
			b := make([]byte, len(e.val))
			copy(b, e.val)
			out[k] = b
			c.hits.Add(1)
		} else {
			c.misses.Add(1)
		}
		s.mu.Unlock()
	}
	return out, nil
}

// Has implements cache.Cache.
func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	if c.closed.Load() {
		return false, cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := c.getEntry(s, key, c.now())
	return ok, nil
}

// TTL implements cache.Cache.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := c.getEntry(s, key, c.now())
	if !ok {
		return 0, cache.ErrNotFound
	}
	if e.exp.IsZero() {
		return 0, nil
	}
	return e.exp.Sub(c.now()), nil
}

// Set implements cache.Cache.
func (c *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	b := make([]byte, len(val))
	copy(b, val)
	s := c.shardFor(key)
	s.mu.Lock()
	c.store(s, key, b, ttl)
	s.mu.Unlock()
	c.sets.Add(1)
	c.aofAppend(aofRec{Op: aofSet, Key: key, Val: b, TTL: ttl})
	return nil
}

// SetMulti implements cache.Cache.
func (c *Cache) SetMulti(ctx context.Context, items map[string]cache.Item) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	for k, it := range items {
		b := make([]byte, len(it.Value))
		copy(b, it.Value)
		s := c.shardFor(k)
		s.mu.Lock()
		c.store(s, k, b, it.TTL)
		s.mu.Unlock()
		c.sets.Add(1)
		c.aofAppend(aofRec{Op: aofSet, Key: k, Val: b, TTL: it.TTL})
	}
	return nil
}

// SetNX implements cache.Cache.
func (c *Cache) SetNX(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error) {
	if c.closed.Load() {
		return false, cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := c.getEntry(s, key, c.now()); ok {
		return false, nil
	}
	b := make([]byte, len(val))
	copy(b, val)
	c.store(s, key, b, ttl)
	c.sets.Add(1)
	c.aofAppend(aofRec{Op: aofSet, Key: key, Val: b, TTL: ttl})
	return true, nil
}

// Expire implements cache.Cache.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := c.getEntry(s, key, c.now())
	if !ok {
		return cache.ErrNotFound
	}
	if ttl > 0 {
		e.exp = c.now().Add(ttl)
	} else {
		e.exp = time.Time{}
	}
	c.aofAppend(aofRec{Op: aofExpire, Key: key, TTL: ttl})
	return nil
}

// Touch implements cache.Cache.
func (c *Cache) Touch(ctx context.Context, key string) error {
	return c.Expire(ctx, key, time.Hour)
}

func (c *Cache) addInt(key string, delta int64) (int64, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur int64
	var exp time.Duration
	if e, ok := c.getEntry(s, key, c.now()); ok {
		if len(e.val) == 8 {
			cur = int64(binary.BigEndian.Uint64(e.val))
		}
		if !e.exp.IsZero() {
			exp = e.exp.Sub(c.now())
		}
	}
	cur += delta
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(cur))
	c.store(s, key, b, exp)
	c.aofAppend(aofRec{Op: aofSet, Key: key, Val: b, TTL: exp})
	return cur, nil
}

// Incr implements cache.Cache.
func (c *Cache) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return c.addInt(key, delta)
}

// Decr implements cache.Cache.
func (c *Cache) Decr(ctx context.Context, key string, delta int64) (int64, error) {
	return c.addInt(key, -delta)
}

// Del implements cache.Cache.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	for _, k := range keys {
		s := c.shardFor(k)
		s.mu.Lock()
		if _, ok := s.items[k]; ok {
			c.deleteEntry(s, k, cache.EvictExplicit)
			c.dels.Add(1)
			s.mu.Unlock()
			c.aofAppend(aofRec{Op: aofDel, Key: k})
			continue
		}
		s.mu.Unlock()
	}
	return nil
}

// DeleteByPrefix implements cache.Cache.
func (c *Cache) DeleteByPrefix(ctx context.Context, prefix string) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	for _, s := range c.shards {
		s.mu.Lock()
		var deleted []string
		for k := range s.items {
			if strings.HasPrefix(k, prefix) {
				c.deleteEntry(s, k, cache.EvictExplicit)
				c.dels.Add(1)
				deleted = append(deleted, k)
			}
		}
		s.mu.Unlock()
		for _, k := range deleted {
			c.aofAppend(aofRec{Op: aofDel, Key: k})
		}
	}
	return nil
}

// Flush implements cache.Cache.
func (c *Cache) Flush(ctx context.Context) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	for _, s := range c.shards {
		s.mu.Lock()
		s.items = map[string]*entry{}
		s.bytes = 0
		s.pol.clear()
		s.mu.Unlock()
	}
	c.aofAppend(aofRec{Op: aofFlush})
	return nil
}

// Iterate implements cache.Cache. It snapshots the matching keys under each
// shard lock, then yields values lazily via Get. Because the value fetch
// happens after the key snapshot, an entry deleted or expired between the two
// is silently skipped by iter.Next (no error, no nil value) — iteration is a
// best-effort point-in-time view, not a consistent transaction.
func (c *Cache) Iterate(ctx context.Context, opts cache.IterateOpts) cache.Iterator {
	var keys []string
	now := c.now()
	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			if expired(e, now) {
				continue
			}
			if strings.HasPrefix(k, opts.Prefix) {
				keys = append(keys, k)
			}
		}
		s.mu.Unlock()
	}
	return &iter{c: c, keys: keys, pos: -1}
}

type iter struct {
	c    *Cache
	keys []string
	pos  int
	k    string
	v    []byte
}

func (it *iter) Next() bool {
	for {
		it.pos++
		if it.pos >= len(it.keys) {
			return false
		}
		v, err := it.c.Get(context.Background(), it.keys[it.pos])
		if err == nil {
			it.k, it.v = it.keys[it.pos], v
			return true
		}
	}
}

func (it *iter) Key() string   { return it.k }
func (it *iter) Value() []byte { return it.v }
func (it *iter) Err() error    { return nil }
func (it *iter) Close() error  { return nil }

// Ping implements cache.Cache.
func (c *Cache) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	return nil
}

// Close implements cache.Cache. It is idempotent: the first call flips the
// closed guard, writes a final checkpoint (guard-free path so it still works
// while closed), stops the background loops, and closes the AOF handle;
// subsequent calls are no-ops. After Close every public op returns
// cache.ErrClosed.
func (c *Cache) Close() error {
	if c.closed.Swap(true) {
		return nil // idempotent
	}
	// Final checkpoint before tearing down (bypasses the closed guard).
	if c.cfg.ckptPath != "" && c.cfg.ckptInterval > 0 {
		_ = c.writeCheckpoint()
	}
	close(c.stop)
	return c.aof.close()
}

// Stats implements cache.Cache. Counters are atomics read without a global
// lock; entries/bytes are summed by briefly locking each shard in turn (never
// all at once, so it cannot deadlock against a single-shard op). The returned
// EvictionsByCause is a defensive copy of the internal map.
func (c *Cache) Stats() cache.Stats {
	var entries, bytes int64
	for _, s := range c.shards {
		s.mu.Lock()
		entries += int64(len(s.items))
		bytes += s.bytes
		s.mu.Unlock()
	}
	c.evCauseMu.Lock()
	byCause := make(map[cache.EvictionCause]int64, len(c.evCause))
	for k, v := range c.evCause {
		byCause[k] = v
	}
	c.evCauseMu.Unlock()
	return cache.Stats{
		Hits:             c.hits.Load(),
		Misses:           c.misses.Load(),
		Sets:             c.sets.Load(),
		Deletes:          c.dels.Load(),
		Evictions:        c.evicts.Load(),
		EvictionsByCause: byCause,
		Entries:          entries,
		Bytes:            bytes,
	}
}

// sweepLoop proactively reclaims expired entries every cfg.sweepInterval.
// Without it, expired entries are only dropped lazily when next accessed
// (getEntry) or scanned. Stopped when Close closes c.stop.
func (c *Cache) sweepLoop() {
	t := time.NewTicker(c.cfg.sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			now := c.now()
			for _, s := range c.shards {
				s.mu.Lock()
				for k, e := range s.items {
					if expired(e, now) {
						c.deleteEntry(s, k, cache.EvictExpired)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}

var _ cache.Cache = (*Cache)(nil)
