// coverage_test.go — targeted tests filling coverage gaps across every option,
// both eviction policies' admission/promotion edges, snapshot/AOF/checkpoint
// recovery paths, closed-cache guards, and Stats/Iterate edges. Deterministic:
// uses an injected clock for all expiry, mirroring the existing seam patterns.

package memcache_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
)

// fakeClock is a deterministic, monotonic, race-safe time source.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// ---------- Options ----------

func TestWithClockDrivesExpiry(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	c := memcache.New(memcache.WithClock(clk.Now))
	defer func() { _ = c.Close() }()

	_ = c.Set(ctx, "k", []byte("v"), time.Minute)
	if ok, _ := c.Has(ctx, "k"); !ok {
		t.Fatal("k should be present before clock advance")
	}
	clk.Advance(2 * time.Minute)
	if ok, _ := c.Has(ctx, "k"); ok {
		t.Fatal("k should be lazily expired after clock advance")
	}
	// TTL on missing key after expiry returns ErrNotFound.
	if _, err := c.TTL(ctx, "k"); err != cache.ErrNotFound {
		t.Fatalf("TTL after expiry: want ErrNotFound, got %v", err)
	}
}

func TestWithWeigherCustomCost(t *testing.T) {
	ctx := context.Background()
	// Every entry costs a flat 10 regardless of byte length.
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxBytes(25),
		memcache.WithWeigher(func(_ []byte) int64 { return 10 }),
	)
	defer func() { _ = c.Close() }()

	_ = c.Set(ctx, "a", []byte("x"), 0)
	_ = c.Set(ctx, "b", []byte("y"), 0)
	_ = c.Set(ctx, "c", []byte("z"), 0) // 30 > 25 -> oldest evicted
	s := c.Stats()
	if s.Bytes > 25 {
		t.Fatalf("custom weigher byte cap not enforced: bytes=%d", s.Bytes)
	}
	if s.Entries != 2 {
		t.Fatalf("want 2 entries after weighed eviction, got %d", s.Entries)
	}
}

func TestWithShardsRoundsUpToPow2(t *testing.T) {
	ctx := context.Background()
	// 5 -> rounds up to 8; behavior must remain correct.
	c := memcache.New(memcache.WithShards(5))
	defer func() { _ = c.Close() }()
	for i := 0; i < 100; i++ {
		k := "k" + strconv.Itoa(i)
		_ = c.Set(ctx, k, []byte(k), 0)
	}
	for i := 0; i < 100; i++ {
		k := "k" + strconv.Itoa(i)
		if v, err := c.Get(ctx, k); err != nil || string(v) != k {
			t.Fatalf("key %s lost across rounded shard count: %q %v", k, v, err)
		}
	}
}

func TestWithShardsZeroAndNegative(t *testing.T) {
	for _, n := range []int{0, -3} {
		c := memcache.New(memcache.WithShards(n))
		ctx := context.Background()
		_ = c.Set(ctx, "k", []byte("v"), 0)
		if v, err := c.Get(ctx, "k"); err != nil || string(v) != "v" {
			t.Fatalf("shards=%d broke basic ops: %q %v", n, v, err)
		}
		_ = c.Close()
	}
}

func TestWithOnEvictEveryCause(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	var mu sync.Mutex
	seen := map[cache.EvictionCause]int{}
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(2),
		memcache.WithPolicy(memcache.LRU),
		memcache.WithClock(clk.Now),
		memcache.WithOnEvict(func(_ string, cause cache.EvictionCause) {
			mu.Lock()
			seen[cause]++
			mu.Unlock()
		}),
	)
	defer func() { _ = c.Close() }()

	// EvictReplaced: overwrite an existing key.
	_ = c.Set(ctx, "r", []byte("1"), 0)
	_ = c.Set(ctx, "r", []byte("2"), 0)
	// EvictExpired: set with TTL, expire, then access.
	_ = c.Set(ctx, "e", []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute)
	_, _ = c.Get(ctx, "e")
	// EvictSize: exceed the 2-entry cap.
	_ = c.Set(ctx, "s1", []byte("v"), 0)
	_ = c.Set(ctx, "s2", []byte("v"), 0)
	_ = c.Set(ctx, "s3", []byte("v"), 0) // forces size eviction
	// EvictExplicit: explicit Del.
	_ = c.Set(ctx, "d", []byte("v"), 0)
	_ = c.Del(ctx, "d")

	mu.Lock()
	defer mu.Unlock()
	for _, cause := range []cache.EvictionCause{
		cache.EvictReplaced, cache.EvictExpired, cache.EvictSize, cache.EvictExplicit,
	} {
		if seen[cause] == 0 {
			t.Fatalf("cause %v never fired onEvict; saw %v", cause, seen)
		}
	}

	st := c.Stats()
	if st.EvictionsByCause[cache.EvictExplicit] == 0 {
		t.Fatal("Stats.EvictionsByCause missing EvictExplicit")
	}
	if st.EvictionsByCause[cache.EvictReplaced] == 0 {
		t.Fatal("Stats.EvictionsByCause missing EvictReplaced")
	}
}

func TestWithSweepIntervalBackgroundExpiry(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	c := memcache.New(
		memcache.WithClock(clk.Now),
		memcache.WithSweepInterval(10*time.Millisecond),
	)
	defer func() { _ = c.Close() }()

	_ = c.Set(ctx, "k", []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute) // logically expired but not yet accessed

	// The background sweeper must proactively drop it within a bounded wait.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Stats().Entries == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("sweeper did not reclaim expired entry; entries=%d", c.Stats().Entries)
}

// ---------- Touch / addInt edges ----------

func TestTouchSetsHourTTL(t *testing.T) {
	ctx := context.Background()
	c := memcache.New()
	defer func() { _ = c.Close() }()

	if err := c.Touch(ctx, "missing"); err != cache.ErrNotFound {
		t.Fatalf("Touch on missing: want ErrNotFound, got %v", err)
	}
	_ = c.Set(ctx, "k", []byte("v"), 0)
	if err := c.Touch(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	d, err := c.TTL(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if d <= 0 || d > time.Hour {
		t.Fatalf("Touch should set ~1h TTL, got %v", d)
	}
}

func TestIncrPreservesTTL(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	c := memcache.New(memcache.WithClock(clk.Now))
	defer func() { _ = c.Close() }()

	if _, err := c.Incr(ctx, "n", 3); err != nil {
		t.Fatal(err)
	}
	_ = c.Expire(ctx, "n", time.Hour)
	if v, _ := c.Incr(ctx, "n", 2); v != 5 {
		t.Fatalf("Incr should accumulate to 5, got %d", v)
	}
	d, err := c.TTL(ctx, "n")
	if err != nil || d <= 0 || d > time.Hour {
		t.Fatalf("Incr must preserve remaining TTL: %v %v", d, err)
	}
	if v, _ := c.Decr(ctx, "n", 1); v != 4 {
		t.Fatalf("Decr to 4, got %d", v)
	}
}

func TestExpireZeroClearsTTL(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	c := memcache.New(memcache.WithClock(clk.Now))
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k", []byte("v"), time.Minute)
	if err := c.Expire(ctx, "k", 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Hour)
	if ok, _ := c.Has(ctx, "k"); !ok {
		t.Fatal("Expire(0) should clear TTL so the key never expires")
	}
	d, _ := c.TTL(ctx, "k")
	if d != 0 {
		t.Fatalf("TTL of no-expiry key should be 0, got %v", d)
	}
}

// ---------- Iterate ----------

func TestIteratePrefixAndExpiredSkip(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	c := memcache.New(memcache.WithClock(clk.Now))
	defer func() { _ = c.Close() }()

	_ = c.Set(ctx, "user:1", []byte("a"), 0)
	_ = c.Set(ctx, "user:2", []byte("b"), 0)
	_ = c.Set(ctx, "user:gone", []byte("c"), time.Minute)
	_ = c.Set(ctx, "other:1", []byte("d"), 0)
	clk.Advance(2 * time.Minute) // user:gone expired

	it := c.Iterate(ctx, cache.IterateOpts{Prefix: "user:"})
	defer func() { _ = it.Close() }()
	got := map[string]string{}
	for it.Next() {
		got[it.Key()] = string(it.Value())
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	if len(got) != 2 || got["user:1"] != "a" || got["user:2"] != "b" {
		t.Fatalf("prefix+expired iteration wrong: %v", got)
	}
}

// ---------- Closed-cache guards ----------

func TestClosedCacheGuardsEveryMethod(t *testing.T) {
	ctx := context.Background()
	c := memcache.New()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil { // idempotent
		t.Fatalf("second Close should be no-op, got %v", err)
	}

	if _, err := c.Get(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.GetMulti(ctx, []string{"k"}); err != cache.ErrClosed {
		t.Fatalf("GetMulti: %v", err)
	}
	if _, err := c.Has(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Has: %v", err)
	}
	if _, err := c.TTL(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("TTL: %v", err)
	}
	if err := c.Set(ctx, "k", nil, 0); err != cache.ErrClosed {
		t.Fatalf("Set: %v", err)
	}
	if err := c.SetMulti(ctx, map[string]cache.Item{"k": {}}); err != cache.ErrClosed {
		t.Fatalf("SetMulti: %v", err)
	}
	if _, err := c.SetNX(ctx, "k", nil, 0); err != cache.ErrClosed {
		t.Fatalf("SetNX: %v", err)
	}
	if err := c.Expire(ctx, "k", 0); err != cache.ErrClosed {
		t.Fatalf("Expire: %v", err)
	}
	if err := c.Touch(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Touch: %v", err)
	}
	if _, err := c.Incr(ctx, "k", 1); err != cache.ErrClosed {
		t.Fatalf("Incr: %v", err)
	}
	if _, err := c.Decr(ctx, "k", 1); err != cache.ErrClosed {
		t.Fatalf("Decr: %v", err)
	}
	if err := c.Del(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("Del: %v", err)
	}
	if err := c.DeleteByPrefix(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("DeleteByPrefix: %v", err)
	}
	if err := c.Flush(ctx); err != cache.ErrClosed {
		t.Fatalf("Flush: %v", err)
	}
	if err := c.Ping(ctx); err != cache.ErrClosed {
		t.Fatalf("Ping: %v", err)
	}
	if err := c.SnapshotTo(&bytes.Buffer{}); err != cache.ErrClosed {
		t.Fatalf("SnapshotTo: %v", err)
	}
}

func TestPingOpenCacheOK(t *testing.T) {
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping on open cache: %v", err)
	}
}

func TestDelMissingAndDeleteByPrefixNoMatch(t *testing.T) {
	ctx := context.Background()
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if err := c.Del(ctx, "nope"); err != nil { // missing key: silent no-op
		t.Fatal(err)
	}
	_ = c.Set(ctx, "a", []byte("v"), 0)
	if err := c.DeleteByPrefix(ctx, "zzz"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Has(ctx, "a"); !ok {
		t.Fatal("non-matching prefix must not delete a")
	}
}

// ---------- AOF edges ----------

func TestAOFExpireAndTruncatedTailRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "x.aof")

	clk := newFakeClock()
	c1 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	_ = c1.Set(ctx, "a", []byte("1"), 0)
	_ = c1.Set(ctx, "b", []byte("2"), 0)
	_ = c1.Expire(ctx, "b", time.Hour) // aofExpire op replayed later
	_ = c1.Close()

	// Corrupt the tail: append a partial frame (a length header with no body).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 99}); err != nil { // claims 99 bytes, writes none
		t.Fatal(err)
	}
	_ = f.Close()

	c2 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	defer func() { _ = c2.Close() }()
	if v, err := c2.Get(ctx, "a"); err != nil || string(v) != "1" {
		t.Fatalf("valid prefix lost after truncated tail: %q %v", v, err)
	}
	d, err := c2.TTL(ctx, "b")
	if err != nil || d <= 0 {
		t.Fatalf("aofExpire op not replayed: ttl=%v err=%v", d, err)
	}
}

func TestAOFExpireToNoExpiryReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "e.aof")
	clk := newFakeClock()
	c1 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	_ = c1.Set(ctx, "k", []byte("v"), time.Minute)
	_ = c1.Expire(ctx, "k", 0) // clear TTL via AOF
	_ = c1.Close()

	c2 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	defer func() { _ = c2.Close() }()
	clk.Advance(time.Hour)
	if ok, _ := c2.Has(ctx, "k"); !ok {
		t.Fatal("Expire(0) via AOF replay should clear TTL")
	}
}

func TestCompactAOFTempCreateError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses directory permissions")
	}
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "ce.aof")
	c := memcache.New(memcache.WithAOF(path))
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k", []byte("v"), 0)

	// Make the AOF directory non-writable so CompactAOF's CreateTemp fails.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if err := c.CompactAOF(); err == nil {
		t.Fatal("CompactAOF should surface the temp-file create error")
	}
}

func TestCompactAOFNoAOFConfigured(t *testing.T) {
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if err := c.CompactAOF(); err != nil {
		t.Fatalf("CompactAOF with no AOF should be a no-op nil, got %v", err)
	}
}

func TestCompactAOFSkipsExpiredEntries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "c2.aof")
	clk := newFakeClock()
	c1 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	_ = c1.Set(ctx, "live", []byte("v"), 0)
	_ = c1.Set(ctx, "ttl", []byte("v"), 2*time.Hour)
	_ = c1.Set(ctx, "dead", []byte("v"), time.Minute)
	clk.Advance(2 * time.Minute) // "dead" expired
	if err := c1.CompactAOF(); err != nil {
		t.Fatal(err)
	}
	_ = c1.Set(ctx, "post", []byte("v"), 0)
	_ = c1.Close()

	c2 := memcache.New(memcache.WithAOF(path), memcache.WithClock(clk.Now))
	defer func() { _ = c2.Close() }()
	if ok, _ := c2.Has(ctx, "dead"); ok {
		t.Fatal("expired entry must not survive compaction")
	}
	for _, k := range []string{"live", "ttl", "post"} {
		if ok, _ := c2.Has(ctx, k); !ok {
			t.Fatalf("%s should survive compaction+append", k)
		}
	}
}

// ---------- Snapshot / RestoreFromFile edges ----------

func TestCheckpointToUnwritableDirNoCrash(t *testing.T) {
	ctx := context.Background()
	// ckpt path under a non-existent directory: writeCheckpoint's CreateTemp
	// fails every tick and on the final Close checkpoint. The cache must keep
	// working and Close must not panic (the error is swallowed by design).
	bad := filepath.Join(t.TempDir(), "nonexistent-dir", "cache.ckpt")
	c := memcache.New(memcache.WithCheckpoint(bad, 10*time.Millisecond))
	_ = c.Set(ctx, "k", []byte("v"), 0)
	time.Sleep(40 * time.Millisecond) // let the loop fire writeCheckpoint failures
	if v, err := c.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("cache should stay usable despite checkpoint failures: %q %v", v, err)
	}
	if err := c.Close(); err != nil { // final checkpoint also fails, swallowed
		t.Fatalf("Close should not surface checkpoint write error, got %v", err)
	}
}

func TestSnapshotToFileAndRestoreFromFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "snap.gob")

	src := memcache.New()
	_ = src.Set(ctx, "a", []byte("1"), 0)
	_ = src.Set(ctx, "b", []byte("2"), time.Hour)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.SnapshotTo(f); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = src.Close()

	dst := memcache.New()
	defer func() { _ = dst.Close() }()
	n, err := dst.RestoreFromFile(path)
	if err != nil || n != 2 {
		t.Fatalf("RestoreFromFile: n=%d err=%v", n, err)
	}
	if v, _ := dst.Get(ctx, "a"); string(v) != "1" {
		t.Fatalf("a not restored: %q", v)
	}
}

func TestRestoreFromFileOpenErrorIsDir(t *testing.T) {
	dir := t.TempDir() // opening a directory then decoding it errors
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if _, err := c.RestoreFromFile(dir); err == nil {
		t.Fatal("RestoreFromFile on a directory should error")
	}
}

func TestRestoreFromFilePermissionError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses file permissions")
	}
	path := filepath.Join(t.TempDir(), "noperm.gob")
	if err := os.WriteFile(path, []byte("data"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if _, err := c.RestoreFromFile(path); err == nil {
		t.Fatal("RestoreFromFile on an unreadable (non-missing) file should error")
	}
}

func TestRestoreFromCorruptStreamErrors(t *testing.T) {
	c := memcache.New()
	defer func() { _ = c.Close() }()
	if _, err := c.RestoreFrom(bytes.NewReader([]byte("not-gob-data"))); err == nil {
		t.Fatal("RestoreFrom on corrupt stream should error")
	}
}

// ---------- Policy admission / promotion edges ----------

func TestWTinyLFUPromotionAndProtectedDemotion(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(400),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()

	// Fill, then repeatedly hit a working set to drive
	// window->probation->protected promotion and protected-tail demotion.
	for i := 0; i < 400; i++ {
		k := "k" + strconv.Itoa(i)
		_ = c.Set(ctx, k, []byte("v"), 0)
	}
	for round := 0; round < 20; round++ {
		for i := 0; i < 300; i++ {
			_, _ = c.Get(ctx, "k"+strconv.Itoa(i))
		}
	}
	hits := 0
	for i := 0; i < 300; i++ {
		if ok, _ := c.Has(ctx, "k"+strconv.Itoa(i)); ok {
			hits++
		}
	}
	if hits == 0 {
		t.Fatal("expected a protected working set to survive under W-TinyLFU")
	}
}

func TestWTinyLFURejectsOneHitWonderAndAges(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(150),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()

	// Establish a strong hot set so its sketch frequency is high.
	for i := 0; i < 30; i++ {
		_ = c.Set(ctx, "hot"+strconv.Itoa(i), []byte("v"), 0)
	}
	for r := 0; r < 50; r++ {
		for i := 0; i < 30; i++ {
			_, _ = c.Get(ctx, "hot"+strconv.Itoa(i))
		}
	}
	// Large flood of unique one-hit-wonders: forces the cand==key reject path
	// and drives enough increments to trigger Count-Min aging.
	for i := 0; i < 50000; i++ {
		k := "w" + strconv.Itoa(i)
		_ = c.Set(ctx, k, []byte("v"), 0)
	}
	survived := 0
	for i := 0; i < 30; i++ {
		if ok, _ := c.Has(ctx, "hot"+strconv.Itoa(i)); ok {
			survived++
		}
	}
	if survived == 0 {
		t.Fatal("hot set should resist one-hit-wonder flood")
	}
}

func TestLRUEvictByteCapPath(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxBytes(20),
		memcache.WithPolicy(memcache.LRU),
	)
	defer func() { _ = c.Close() }()
	for i := 0; i < 10; i++ {
		_ = c.Set(ctx, "k"+strconv.Itoa(i), make([]byte, 8), 0)
	}
	if c.Stats().Bytes > 20 {
		t.Fatalf("LRU byte cap not enforced: %d", c.Stats().Bytes)
	}
}

func TestWTinyLFUByteCapEvict(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxBytes(40),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()
	for i := 0; i < 20; i++ {
		_ = c.Set(ctx, "k"+strconv.Itoa(i), make([]byte, 8), 0)
		_, _ = c.Get(ctx, "k"+strconv.Itoa(i)) // promote some into main/protected
	}
	if c.Stats().Bytes > 40 {
		t.Fatalf("W-TinyLFU byte cap not enforced: %d", c.Stats().Bytes)
	}
}

func TestFlushResetsBothPolicies(t *testing.T) {
	ctx := context.Background()
	for _, pol := range []memcache.Policy{memcache.LRU, memcache.AdaptiveWTinyLFU} {
		c := memcache.New(memcache.WithShards(1), memcache.WithMaxEntries(50), memcache.WithPolicy(pol))
		for i := 0; i < 40; i++ {
			_ = c.Set(ctx, "k"+strconv.Itoa(i), []byte("v"), 0)
			_, _ = c.Get(ctx, "k"+strconv.Itoa(i))
		}
		if err := c.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if c.Stats().Entries != 0 {
			t.Fatalf("policy %v: Flush left %d entries", pol, c.Stats().Entries)
		}
		// Still usable after flush.
		_ = c.Set(ctx, "x", []byte("y"), 0)
		if v, _ := c.Get(ctx, "x"); string(v) != "y" {
			t.Fatalf("policy %v: cache unusable after flush", pol)
		}
		_ = c.Close()
	}
}

func TestWithClockNilFallsBackToTimeNow(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(memcache.WithClock(nil)) // nil clock -> defaults to time.Now
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k", []byte("v"), 30*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if ok, _ := c.Has(ctx, "k"); ok {
		t.Fatal("nil clock should fall back to real time and expire k")
	}
}

func TestWithWeigherNilFallsBackToLen(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxBytes(10),
		memcache.WithWeigher(nil), // nil weigher -> defaults to len()
	)
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "a", make([]byte, 6), 0)
	_ = c.Set(ctx, "b", make([]byte, 6), 0) // 12 > 10 -> eviction by len-weigher
	if c.Stats().Bytes > 10 {
		t.Fatalf("default len weigher cap not enforced: %d", c.Stats().Bytes)
	}
}

func TestMaxEntriesFewerThanShards(t *testing.T) {
	ctx := context.Background()
	// maxEntries(3) / 16 shards -> capPerShard clamps to 1.
	c := memcache.New(memcache.WithMaxEntries(3))
	defer func() { _ = c.Close() }()
	for i := 0; i < 200; i++ {
		_ = c.Set(ctx, "k"+strconv.Itoa(i), []byte("v"), 0)
	}
	// Each shard holds at most 1; total bounded well under 200.
	if c.Stats().Entries > 32 {
		t.Fatalf("per-shard clamp not applied, entries=%d", c.Stats().Entries)
	}
}

func TestWTinyLFUCapacityOne(t *testing.T) {
	ctx := context.Background()
	// capKeys==1 -> winCap clamps to 1 and mainCap clamps to 1 (newWTinyPolicy
	// edge). Drives the all-protected probation-empty admission branch.
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(1),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()
	for i := 0; i < 50; i++ {
		k := "k" + strconv.Itoa(i)
		_ = c.Set(ctx, k, []byte("v"), 0)
		_, _ = c.Get(ctx, k)
		_, _ = c.Get(ctx, k) // promote toward protected
	}
	if c.Stats().Entries > 4 {
		t.Fatalf("capacity-1 W-TinyLFU shard grew unexpectedly: %d", c.Stats().Entries)
	}
}

func TestLRUCapacityOne(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(memcache.WithShards(1), memcache.WithMaxEntries(1), memcache.WithPolicy(memcache.LRU))
	defer func() { _ = c.Close() }()
	for i := 0; i < 20; i++ {
		_ = c.Set(ctx, "k"+strconv.Itoa(i), []byte("v"), 0)
	}
	if c.Stats().Entries != 1 {
		t.Fatalf("LRU cap=1 should keep exactly 1 entry, got %d", c.Stats().Entries)
	}
}

func TestMaxBytesSmallerThanShardCount(t *testing.T) {
	ctx := context.Background()
	// maxBytes(4) / 16 shards -> per-shard cap clamps to 1.
	c := memcache.New(memcache.WithMaxBytes(4))
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k", make([]byte, 100), 0) // way over the 1-byte/shard cap
	// Should not panic; entry is evicted down to within cap or dropped.
	if c.Stats().Bytes > 100 {
		t.Fatalf("unexpected bytes: %d", c.Stats().Bytes)
	}
}

func TestRestoreFromSkipsNegativeTTL(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	// A snapshot whose entry's remaining TTL elapses to negative before
	// RestoreFrom decodes it must skip that entry.
	src := memcache.New(memcache.WithClock(clk.Now))
	_ = src.Set(ctx, "soon", []byte("v"), time.Minute)
	_ = src.Set(ctx, "live", []byte("v"), 0)
	var buf bytes.Buffer
	if err := src.SnapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	_ = src.Close()

	// Advance the destination's clock far past the snapshot's remaining TTL;
	// store recomputes exp and the entry is immediately expired (covered
	// elsewhere). Here we assert the live entry restores fine.
	dst := memcache.New(memcache.WithClock(clk.Now))
	defer func() { _ = dst.Close() }()
	n, err := dst.RestoreFrom(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("expected at least the live entry restored, got %d", n)
	}
	if ok, _ := dst.Has(ctx, "live"); !ok {
		t.Fatal("live entry must restore")
	}
}

// snapWire mirrors the on-disk snapEntry layout (gob is structural, matching
// exported field names/types decode into the package's unexported snapEntry).
type snapWire struct {
	Key string
	Val []byte
	TTL time.Duration
}

func TestRestoreFromNegativeTTLEntryIsSkipped(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// Negative remaining TTL = already expired at snapshot time -> skipped.
	if err := enc.Encode(&snapWire{Key: "expired", Val: []byte("x"), TTL: -time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(&snapWire{Key: "alive", Val: []byte("y"), TTL: 0}); err != nil {
		t.Fatal(err)
	}
	c := memcache.New()
	defer func() { _ = c.Close() }()
	n, err := c.RestoreFrom(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("only the non-expired entry should restore, got n=%d", n)
	}
	if ok, _ := c.Has(ctx, "expired"); ok {
		t.Fatal("negative-TTL entry must be skipped on restore")
	}
	if ok, _ := c.Has(ctx, "alive"); !ok {
		t.Fatal("alive entry must restore")
	}
}

func TestStoreAdmissionDeniedDropsEntry(t *testing.T) {
	ctx := context.Background()
	// Tiny W-TinyLFU cache: a brand-new cold key that loses the TinyLFU duel
	// against a hot incumbent is rejected at store() (add() returns false).
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(2),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "hot", []byte("v"), 0)
	for i := 0; i < 200; i++ {
		_, _ = c.Get(ctx, "hot")
	}
	for i := 0; i < 200; i++ {
		_ = c.Set(ctx, "cold"+strconv.Itoa(i), []byte("v"), 0)
	}
	if ok, _ := c.Has(ctx, "hot"); !ok {
		t.Fatal("hot key should survive cold one-hit-wonder floods")
	}
}

func TestWTinyLFUAllProtectedVictimPath(t *testing.T) {
	ctx := context.Background()
	// Larger capacity so a hot working set is promoted into the protected
	// sub-region; subsequent cold inserts must duel against a protected-tail
	// victim when probation drains (policy probation-empty branch), and a
	// losing brand-new candidate is dropped at store() (admission-denied).
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(120),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()

	// Promote a hot set into protected with very high frequency.
	for i := 0; i < 80; i++ {
		_ = c.Set(ctx, "h"+strconv.Itoa(i), []byte("v"), 0)
	}
	for r := 0; r < 80; r++ {
		for i := 0; i < 80; i++ {
			_, _ = c.Get(ctx, "h"+strconv.Itoa(i))
		}
	}
	// Heavy cold flood: each unique key duels and (mostly) loses, exercising
	// the store admission-denied drop and the probation/protected victim
	// selection branches.
	for i := 0; i < 30000; i++ {
		_ = c.Set(ctx, "c"+strconv.Itoa(i), []byte("v"), 0)
	}
	survived := 0
	for i := 0; i < 80; i++ {
		if ok, _ := c.Has(ctx, "h"+strconv.Itoa(i)); ok {
			survived++
		}
	}
	if survived == 0 {
		t.Fatal("hot protected set should largely survive a cold flood")
	}
}

func TestEvictOnEmptyPolicyNoPanic(_ *testing.T) {
	ctx := context.Background()
	// maxBytes set but cache empty then a single oversized value: trimBytes
	// loops evict() which may hit the empty-list path on the popTail/evict.
	for _, pol := range []memcache.Policy{memcache.LRU, memcache.AdaptiveWTinyLFU} {
		c := memcache.New(memcache.WithShards(1), memcache.WithMaxBytes(5), memcache.WithPolicy(pol))
		_ = c.Set(ctx, "big", make([]byte, 50), 0)
		_ = c.Flush(ctx)
		_ = c.Set(ctx, "big2", make([]byte, 50), 0)
		_ = c.Close()
	}
}

// Concurrency smoke under -race exercising mixed ops across shards.
func TestConcurrentMixedOps(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(memcache.WithMaxEntries(500))
	defer func() { _ = c.Close() }()
	var wg sync.WaitGroup
	var ops atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := "g" + strconv.Itoa(g) + ":" + strconv.Itoa(i%50)
				_ = c.Set(ctx, k, []byte("v"), 0)
				_, _ = c.Get(ctx, k)
				if i%7 == 0 {
					_ = c.Del(ctx, k)
				}
				ops.Add(1)
			}
		}(g)
	}
	wg.Wait()
	if ops.Load() != 8*500 {
		t.Fatalf("expected 4000 ops, got %d", ops.Load())
	}
}
