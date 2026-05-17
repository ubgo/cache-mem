# Changelog

All notable changes to `github.com/ubgo/cache-mem` are documented here. Format
follows Keep a Changelog; the project follows SemVer (pre-GA in `v0.x`).

## [Unreleased]

### Added

- Sharded, weight-aware in-memory LRU adapter implementing `cache.Cache`.
- Options: `WithShards`, `WithMaxEntries`, `WithMaxBytes`, `WithWeigher`,
  `WithOnEvict`, `WithSweepInterval`, `WithClock`.
- Lazy + background TTL expiry; idempotent `Close` stops the sweeper.
- Full `Stats` (hits/misses/sets/deletes/evictions-by-cause/entries/bytes).
- Passes the shared `github.com/ubgo/cache/cachetest` conformance suite under
  `-race`.

### Phase 1.5

- Pluggable eviction `WithPolicy`: `LRU` and `AdaptiveWTinyLFU`
  (Window-TinyLFU: 1% admission window + segmented-LRU main + 4-bit Count-Min
  sketch with aging).
- **Default policy is now `AdaptiveWTinyLFU`** (PLAN Q6 / §1.5). Pass
  `WithPolicy(memcache.LRU)` for the previous behaviour.
- Conformance passes under `-race` for both policies; a scan-resistance test
  verifies the hot set survives a 5× capacity scan.

### Persistence

- `SnapshotTo(w)` / `RestoreFrom(r)`: gob warm-restart snapshot. Stores
  *remaining* TTL so restored entries never outlive their original deadline;
  expired entries are skipped on both snapshot and restore.
- `WithCheckpoint(path, interval)`: background periodic checkpoint (atomic
  temp-file + rename) plus a final checkpoint on Close; `RestoreFromFile`
  treats a missing file as a clean cold start.
- `WithAOF(path)`: fsync-per-write append-only durability with replay on New;
  length-prefixed self-contained frames (crash-truncation tolerant,
  concatenation-safe). `CompactAOF()` rewrites the log to minimal state.

[Unreleased]: https://github.com/ubgo/cache-mem/commits/main
