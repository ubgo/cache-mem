# Options

All options are passed to `memcache.New`. Zero-valued caps/intervals disable
the corresponding feature (unbounded / lazy-only / no persistence).

### Option

`type Option func(*config)`

What it is: the functional-option type. Every `WithX` returns one.

Use cases:

- Build a reusable option slice for several caches with the same policy.
- Conditionally append options at startup based on configuration.

```go
opts := []memcache.Option{memcache.WithShards(32)}
if persistent {
	opts = append(opts, memcache.WithAOF("cache.aof"))
}
c := memcache.New(opts...)
```

### Weigher

`type Weigher func(val []byte) int64`

What it is: the per-entry cost function used by `WithMaxBytes`. Default counts
raw value bytes (`len(val)`).

Use cases:

- Charge a fixed overhead per entry (e.g. account for key + map overhead).
- Weight by decoded size rather than encoded byte length.

```go
w := memcache.Weigher(func(v []byte) int64 { return int64(len(v)) + 64 })
c := memcache.New(memcache.WithMaxBytes(64<<20), memcache.WithWeigher(w))
```

### WithShards

`func WithShards(n int) Option`

What it is: sets the shard count (default 16). Rounded up to a power of two.

Use cases:

- Raise shard count under heavy concurrent write load to cut mutex contention.
- Lower to 1 for deterministic single-lock behavior in tests.

```go
c := memcache.New(memcache.WithShards(64)) // 64 independently locked shards
```

### WithMaxEntries

`func WithMaxEntries(n int64) Option`

What it is: caps total entries; the cap is split per shard (`n / shards`,
minimum 1). When a shard exceeds its cap the policy evicts.

Use cases:

- Bound memory for predictable, similarly-sized values.
- Keep a fixed-size hot set (e.g. "1M most-recently-used sessions").

```go
c := memcache.New(memcache.WithMaxEntries(1_000_000))
```

### WithMaxBytes

`func WithMaxBytes(n int64) Option`

What it is: caps total weighed size; LRU/policy eviction enforces it
per-shard. Uses the `Weigher` (default: value length).

Use cases:

- Bound memory when value sizes vary widely (HTML fragments, JSON blobs).
- Pair with `WithWeigher` to budget by real footprint.

```go
c := memcache.New(memcache.WithMaxBytes(512 << 20)) // ~512 MiB
```

### WithWeigher

`func WithWeigher(w Weigher) Option`

What it is: overrides the cost function used by `WithMaxBytes`.

Use cases:

- Account for per-entry bookkeeping overhead, not just payload bytes.
- Treat some keys as "free" (return 0) to exempt them from byte pressure.

```go
c := memcache.New(
	memcache.WithMaxBytes(128<<20),
	memcache.WithWeigher(func(v []byte) int64 { return int64(len(v)) + 48 }),
)
```

### WithOnEvict

`func WithOnEvict(fn func(key string, cause cache.EvictionCause)) Option`

What it is: registers a callback fired for every eviction with its cause.
Causes: `cache.EvictSize`, `cache.EvictExpired`, `cache.EvictExplicit`,
`cache.EvictReplaced`.

Use cases:

- Emit metrics segmented by eviction reason.
- Write back a dirty value to durable storage when it is evicted for size.
- Debug churn ("why is this key disappearing?").

```go
import "github.com/ubgo/cache"

c := memcache.New(memcache.WithOnEvict(func(k string, cause cache.EvictionCause) {
	if cause == cache.EvictSize {
		log.Printf("evicted %s under memory pressure", k)
	}
}))
```

### WithSweepInterval

`func WithSweepInterval(d time.Duration) Option`

What it is: runs a background goroutine that proactively drops expired entries
every `d` (in addition to lazy expiry on read). Stopped by `Close`. `0` =
lazy-only.

Use cases:

- Reclaim memory promptly for short-TTL keys that are rarely read again.
- Keep `Stats().Entries` close to the live count for monitoring.

```go
c := memcache.New(memcache.WithSweepInterval(10 * time.Second))
defer c.Close() // stops the sweeper
```

### WithClock

`func WithClock(fn func() time.Time) Option`

What it is: overrides the time source (tests only).

Use cases:

- Deterministically test TTL expiry without `time.Sleep`.
- Simulate clock jumps.

```go
now := time.Now()
c := memcache.New(memcache.WithClock(func() time.Time { return now }))
_ = c.Set(ctx, "k", []byte("v"), time.Minute)
now = now.Add(2 * time.Minute) // key is now expired deterministically
```

### WithPolicy

`func WithPolicy(p Policy) Option`

What it is: selects the eviction algorithm. Default is `AdaptiveWTinyLFU`. See
[Policies](policies.md).

Use cases:

- Keep `AdaptiveWTinyLFU` (default) for skewed / scan-heavy workloads.
- Switch to `LRU` when you want simple, predictable recency behavior.

```go
c := memcache.New(memcache.WithPolicy(memcache.LRU))
```

### WithCheckpoint

`func WithCheckpoint(path string, interval time.Duration) Option`

What it is: periodically snapshots the whole cache to `path` (atomic temp file
+ rename) every `interval`, and writes a final checkpoint on `Close`. Cheap;
lossy to the last checkpoint.

Use cases:

- Warm-start after a deploy/restart instead of a cold cache stampede.
- Cheap durability when losing the last interval of writes is acceptable.

```go
c := memcache.New(memcache.WithCheckpoint("/var/cache/app.snap", time.Minute))
defer c.Close()
// at startup, before serving:
_, _ = c.RestoreFromFile("/var/cache/app.snap")
```

### WithAOF

`func WithAOF(path string) Option`

What it is: append-only-file durability. Every mutating write
(`Set`/`SetMulti`/`SetNX`/`Expire`/`Incr`/`Decr`/`Del`/`Flush`) is
fsync-appended; an existing log is replayed into memory on `New` before
appends resume. Near-zero loss at the cost of a sync per write. Composes with
`WithCheckpoint`. Call `CompactAOF` periodically to bound the log.

Use cases:

- Idempotency keys / dedup state that must survive a crash.
- Near-zero-loss durability without standing up Redis/Postgres.

```go
c := memcache.New(memcache.WithAOF("/var/cache/app.aof"))
defer c.Close()
// existing log already replayed by New; appends resume automatically
go func() {
	for range time.Tick(time.Hour) {
		_ = c.CompactAOF() // bound log growth
	}
}()
```
