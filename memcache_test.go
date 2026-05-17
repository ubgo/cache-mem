package memcache_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
	"github.com/ubgo/cache/cachetest"
)

func TestConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		c := memcache.New(memcache.WithSweepInterval(20 * time.Millisecond))
		t.Cleanup(func() { _ = c.Close() })
		return c
	})
}

func BenchmarkMem(b *testing.B) {
	cachetest.Bench(b, func(_ *testing.B) cache.Cache {
		return memcache.New()
	})
}

func TestConformanceWTinyLFU(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		c := memcache.New(
			memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
			memcache.WithSweepInterval(20*time.Millisecond),
		)
		t.Cleanup(func() { _ = c.Close() })
		return c
	})
}

// W-TinyLFU must protect a frequently-used hot set from a one-shot scan that
// would flush a plain LRU. Hot keys are accessed repeatedly, then a flood of
// unique "scan" keys (each seen once) passes through; the hot set should
// largely survive under W-TinyLFU.
func TestWTinyLFUResistsScan(t *testing.T) {
	ctx := context.Background()
	const capacity = 200
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(capacity),
		memcache.WithPolicy(memcache.AdaptiveWTinyLFU),
	)
	defer func() { _ = c.Close() }()

	hot := make([]string, 50)
	for i := range hot {
		hot[i] = "hot:" + strconv.Itoa(i)
		_ = c.Set(ctx, hot[i], []byte("v"), 0)
	}
	// Build up frequency for the hot set.
	for round := 0; round < 30; round++ {
		for _, k := range hot {
			_, _ = c.Get(ctx, k)
		}
	}
	// One-shot scan: 5x capacity unique keys, each touched once.
	for i := 0; i < capacity*5; i++ {
		k := "scan:" + strconv.Itoa(i)
		_ = c.Set(ctx, k, []byte("v"), 0)
		_, _ = c.Get(ctx, k)
	}

	survived := 0
	for _, k := range hot {
		if ok, _ := c.Has(ctx, k); ok {
			survived++
		}
	}
	if survived < 40 { // >=80% of the hot set retained
		t.Fatalf("W-TinyLFU failed to protect hot set: only %d/50 survived the scan", survived)
	}
}

func TestLRUEviction(t *testing.T) {
	ctx := context.Background()
	var evicted []string
	c := memcache.New(
		memcache.WithShards(1),
		memcache.WithMaxEntries(3),
		memcache.WithPolicy(memcache.LRU),
		memcache.WithOnEvict(func(k string, cause cache.EvictionCause) {
			if cause == cache.EvictSize {
				evicted = append(evicted, k)
			}
		}),
	)
	defer func() { _ = c.Close() }()

	for _, k := range []string{"a", "b", "c"} {
		_ = c.Set(ctx, k, []byte("v"), 0)
	}
	_, _ = c.Get(ctx, "a") // touch a -> b is now LRU
	_ = c.Set(ctx, "d", []byte("v"), 0)

	if len(evicted) != 1 || evicted[0] != "b" {
		t.Fatalf("want LRU evict of b, got %v", evicted)
	}
	if ok, _ := c.Has(ctx, "b"); ok {
		t.Fatal("b should be evicted")
	}
	if ok, _ := c.Has(ctx, "a"); !ok {
		t.Fatal("a was recently used, must survive")
	}
}

func TestMaxBytesEviction(t *testing.T) {
	ctx := context.Background()
	c := memcache.New(memcache.WithShards(1), memcache.WithMaxBytes(10))
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k1", make([]byte, 6), 0)
	_ = c.Set(ctx, "k2", make([]byte, 6), 0) // total 12 > 10 -> k1 evicted
	if ok, _ := c.Has(ctx, "k1"); ok {
		t.Fatal("k1 should be evicted by max-bytes")
	}
	if ok, _ := c.Has(ctx, "k2"); !ok {
		t.Fatal("k2 must remain")
	}
}

func TestStatsTracking(t *testing.T) {
	ctx := context.Background()
	c := memcache.New()
	defer func() { _ = c.Close() }()
	_ = c.Set(ctx, "k", []byte("v"), time.Minute)
	_, _ = c.Get(ctx, "k")
	_, _ = c.Get(ctx, "missing")
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 || s.Sets != 1 {
		t.Fatalf("bad stats: %+v", s)
	}
	if s.HitRatio() != 0.5 {
		t.Fatalf("hit ratio: %v", s.HitRatio())
	}
}
