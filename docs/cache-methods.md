# Cache methods (the `cache.Cache` contract)

Every method below satisfies `github.com/ubgo/cache`.`Cache`. Notes describe
the in-memory adapter's specific behavior. All snippets assume:

```go
ctx := context.Background()
c := memcache.New()
defer c.Close()
```

## Read

### Get

`Get(ctx, key) ([]byte, error)`

What it is: returns a **defensive copy** of the value, or
`(nil, cache.ErrNotFound)` on miss/expiry (never `(nil, nil)`). Expired entries
are lazily dropped on access. Records a hit/miss and a policy hit.

Use cases:

- Read-through caching of expensive computations.
- Session / token lookups.

```go
_ = c.Set(ctx, "k", []byte("v"), 0)
v, err := c.Get(ctx, "k")
if errors.Is(err, cache.ErrNotFound) { /* miss */ }
fmt.Println(string(v)) // v
```

### GetMulti

`GetMulti(ctx, keys) (map[string][]byte, error)`

What it is: batched read; absent keys are simply omitted from the result map.
Each present key counts a hit, each absent one a miss.

Use cases:

- Hydrating a list view (fetch many records at once).
- Reducing per-key call overhead in hot loops.

```go
m, _ := c.GetMulti(ctx, []string{"a", "b", "c"})
for k, v := range m { fmt.Println(k, string(v)) }
```

### Has

`Has(ctx, key) (bool, error)`

What it is: presence check honoring expiry (records a policy hit if present).

Use cases:

- Cheap existence test before an expensive recompute.
- Idempotency guard ("have I processed this id?").

```go
ok, _ := c.Has(ctx, "job:123")
```

### TTL

`TTL(ctx, key) (time.Duration, error)`

What it is: remaining lifetime. Returns `(0, nil)` for a key with no expiry,
`(0, cache.ErrNotFound)` if absent/expired, otherwise the remaining duration.

Use cases:

- Decide whether to proactively refresh a near-expiry value.
- Surface "expires in" to an admin UI.

```go
d, err := c.TTL(ctx, "session:42")
if err == nil && d > 0 && d < 30*time.Second { /* refresh soon */ }
```

## Write

### Set

`Set(ctx, key, val, ttl) error`

What it is: stores a defensive copy. `ttl <= 0` means no expiry. AOF-logged.

```go
_ = c.Set(ctx, "user:42", []byte(`{"name":"alice"}`), 5*time.Minute)
```

### SetMulti

`SetMulti(ctx, map[string]cache.Item) error`

What it is: stores many entries, each with its own TTL via `cache.Item`.

```go
_ = c.SetMulti(ctx, map[string]cache.Item{
	"a": {Value: []byte("1"), TTL: time.Minute},
	"b": {Value: []byte("2")}, // no expiry
})
```

### SetNX

`SetNX(ctx, key, val, ttl) (bool, error)`

What it is: set-if-absent. Returns `(true, nil)` only when it created the key.

Use cases:

- Distributed-ish locks / leader election within a process.
- Write-once idempotency markers.

```go
ok, _ := c.SetNX(ctx, "lock:job", []byte("1"), 30*time.Second)
if ok { /* acquired */ }
```

### Expire

`Expire(ctx, key, ttl) error`

What it is: resets a key's expiry. `ttl <= 0` removes expiry (makes it
permanent). `cache.ErrNotFound` if the key is absent. AOF-logged.

```go
_ = c.Expire(ctx, "session:42", time.Hour) // extend
_ = c.Expire(ctx, "session:42", 0)         // make permanent
```

### Touch

`Touch(ctx, key) error`

What it is: convenience for `Expire(ctx, key, time.Hour)` — bumps the entry to
a one-hour TTL.

Use cases:

- "Keep alive on access" semantics for sessions.

```go
_ = c.Touch(ctx, "session:42")
```

## Counters

### Incr / Decr

`Incr(ctx, key, delta) (int64, error)` · `Decr(ctx, key, delta) (int64, error)`

What it is: atomic counters stored as an 8-byte big-endian int64. A missing
key starts at 0. The original expiry is carried through so incrementing a
TTL'd counter does not extend its life. `Decr(k, d)` == `Incr(k, -d)`; values
may go negative.

Use cases:

- Rate-limit counters, hit counters, inventory decrements.

```go
n, _ := c.Incr(ctx, "rl:ip:1.2.3.4", 1)
if n > 100 { /* throttle */ }
_, _ = c.Decr(ctx, "stock:sku9", 1)
```

## Delete

### Del

`Del(ctx, keys...) error`

What it is: deletes the given keys (absent keys are silently skipped, not an
error). AOF-logged per deleted key.

```go
_ = c.Del(ctx, "a", "b", "c")
```

### DeleteByPrefix

`DeleteByPrefix(ctx, prefix) error`

What it is: scans every shard and deletes keys with the given string prefix.

Use cases:

- Invalidate a whole namespace (`"user:42:"`) on update.

```go
_ = c.DeleteByPrefix(ctx, "user:42:")
```

### Flush

`Flush(ctx) error`

What it is: clears every shard (items + byte accounting + policy state).
AOF-logged as a flush record.

```go
_ = c.Flush(ctx) // empty the entire cache
```

## Iterate

### Iterate

`Iterate(ctx, cache.IterateOpts) cache.Iterator`

What it is: snapshots matching keys under each shard lock, then yields values
lazily via `Get`. A key deleted/expired between snapshot and fetch is silently
skipped — best-effort point-in-time view, not a transaction. `Err()` is always
`nil` for this adapter; always `Close()` the iterator.

Use cases:

- Dump or audit cache contents under a prefix.
- Export keys for a warm-up of another cache.

```go
it := c.Iterate(ctx, cache.IterateOpts{Prefix: "user:"})
defer it.Close()
for it.Next() {
	fmt.Println(it.Key(), string(it.Value()))
}
```

## Lifecycle

### Ping

`Ping(ctx) error` — `nil` while open, `cache.ErrClosed` after `Close`.

### Close

`Close() error` — idempotent; final checkpoint + stop loops + close AOF. See
[Construction](construction.md#close).

### Stats

`Stats() cache.Stats`

What it is: a point-in-time snapshot — hits, misses, sets, deletes, evictions,
`EvictionsByCause` (defensive copy), live `Entries`, and `Bytes`. Counters are
lock-free atomics; entries/bytes are summed by briefly locking each shard.

Use cases:

- Export hit ratio + eviction breakdown to metrics.
- Capacity planning (watch `Bytes` vs `WithMaxBytes`).

```go
s := c.Stats()
fmt.Printf("hit ratio %.3f, entries %d, bytes %d\n",
	s.HitRatio(), s.Entries, s.Bytes)
for cause, n := range s.EvictionsByCause {
	fmt.Printf("evicted[%s]=%d\n", cause, n)
}
```
