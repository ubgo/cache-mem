# Eviction policies

Selected with [`WithPolicy`](options.md#withpolicy). The choice is a pure
runtime option — no API difference.

### Policy

`type Policy int`

What it is: the enum selecting the eviction algorithm. Two values: `LRU` and
`AdaptiveWTinyLFU`.

Use cases:

- Make the policy configurable from an app config flag.

```go
func policyFromEnv() memcache.Policy {
	if os.Getenv("CACHE_POLICY") == "lru" {
		return memcache.LRU
	}
	return memcache.AdaptiveWTinyLFU
}
c := memcache.New(memcache.WithPolicy(policyFromEnv()))
```

### LRU

`const LRU Policy = iota`

What it is: classic least-recently-used. On overflow the least-recently-used
key in the affected shard is evicted.

Use cases:

- Workloads with strong temporal locality and no scan floods.
- When you want predictable, easy-to-reason-about eviction.

```go
c := memcache.New(
	memcache.WithMaxEntries(50_000),
	memcache.WithPolicy(memcache.LRU),
)
```

### AdaptiveWTinyLFU

`const AdaptiveWTinyLFU Policy` (the **default**)

What it is: a Caffeine-style Window-TinyLFU — a small LRU admission window in
front of a segmented-LRU main region, gated by an aging Count-Min frequency
sketch. It resists one-hit-wonder floods and scans far better than plain LRU.

Use cases:

- Skewed access (a small hot set among a large cold keyspace).
- Mixed traffic where batch scans would otherwise flush a plain LRU.
- The general default — leave it unless you have a reason for `LRU`.

```go
// Default — explicit here for clarity.
c := memcache.New(
	memcache.WithMaxEntries(1_000_000),
	memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
)
```
