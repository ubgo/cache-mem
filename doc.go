// Package memcache is the in-memory adapter for github.com/ubgo/cache: a
// sharded, weight-aware cache with lazy + background TTL expiry, two pluggable
// eviction policies, and optional snapshot / checkpoint / append-only-file
// durability.
//
// It implements cache.Cache and passes the shared cachetest.Run conformance
// suite, so it is a drop-in for any code written against the cache contract.
//
//	c := memcache.New(
//	    memcache.WithMaxEntries(100_000),
//	    memcache.WithMaxBytes(256<<20),
//	    memcache.WithOnEvict(func(k string, cause cache.EvictionCause) { ... }),
//	)
//	defer c.Close()
//
// Sharding: keys are distributed across N shards (default 16, rounded up to a
// power of two) by a per-cache seeded maphash, each shard guarded by its own
// mutex with its own independent policy state, so contention stays low under
// concurrent load. Capacity caps (WithMaxEntries / WithMaxBytes) are enforced
// per-shard (global cap / shard count, minimum 1).
//
// Eviction policy is selected with WithPolicy. The default is AdaptiveWTinyLFU
// (a Caffeine-style Window-TinyLFU); LRU is also available. The choice is a
// runtime option with no API difference.
//
// Durability is opt-in and composable: WithCheckpoint periodically writes an
// atomic snapshot (cheap, lossy to the last checkpoint), and WithAOF fsyncs
// every mutating write to an append-only log that is replayed on New
// (near-zero loss). SnapshotTo / RestoreFrom / RestoreFromFile give manual
// warm-restart control. TTLs are stored as remaining duration so a restored
// entry never outlives its original deadline.
package memcache
