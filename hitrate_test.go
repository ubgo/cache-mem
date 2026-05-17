package memcache_test

import (
	"testing"

	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
	"github.com/ubgo/cache/cachetest"
)

func build(t *testing.T, pol memcache.Policy, capEntries int64) cache.Cache {
	t.Helper()
	c := memcache.New(memcache.WithMaxEntries(capEntries), memcache.WithPolicy(pol))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// On a Zipfian workload W-TinyLFU should retain the hot set at least as well
// as LRU (in practice noticeably better at tight capacities).
func TestHitRateZipfianWTinyLFUNotWorseThanLRU(t *testing.T) {
	const (
		keyspace = 100_000
		capacity = 2_000
		ops      = 300_000
		skew     = 1.07
	)
	lruC := build(t, memcache.LRU, capacity)
	wtC := build(t, memcache.AdaptiveWTinyLFU, capacity)

	lru := cachetest.HitRate(lruC, cachetest.NewZipfian(keyspace, skew, 1).Key, ops)
	wt := cachetest.HitRate(wtC, cachetest.NewZipfian(keyspace, skew, 1).Key, ops)

	t.Logf("zipfian hit rate: LRU=%.4f  W-TinyLFU=%.4f", lru, wt)
	if wt+0.01 < lru { // allow tiny noise; W-TinyLFU must not regress
		t.Fatalf("W-TinyLFU (%.4f) regressed vs LRU (%.4f) on Zipfian", wt, lru)
	}
}

// On a scan-heavy workload W-TinyLFU should clearly beat LRU.
func TestHitRateScanWTinyLFUBeatsLRU(t *testing.T) {
	const capacity = 500
	const ops = 200_000

	lru := cachetest.HitRate(build(t, memcache.LRU, capacity),
		cachetest.ScanGen(200, 3, 7), ops)
	wt := cachetest.HitRate(build(t, memcache.AdaptiveWTinyLFU, capacity),
		cachetest.ScanGen(200, 3, 7), ops)

	t.Logf("scan hit rate: LRU=%.4f  W-TinyLFU=%.4f", lru, wt)
	if wt <= lru {
		t.Fatalf("W-TinyLFU (%.4f) should beat LRU (%.4f) on a scan workload", wt, lru)
	}
}

func BenchmarkHitRateZipfian(b *testing.B) {
	for _, tc := range []struct {
		name string
		pol  memcache.Policy
	}{{"LRU", memcache.LRU}, {"WTinyLFU", memcache.AdaptiveWTinyLFU}} {
		b.Run(tc.name, func(b *testing.B) {
			c := memcache.New(memcache.WithMaxEntries(2000), memcache.WithPolicy(tc.pol))
			defer func() { _ = c.Close() }()
			gen := cachetest.NewZipfian(100_000, 1.07, 1).Key
			b.ResetTimer()
			hr := cachetest.HitRate(c, gen, b.N)
			b.ReportMetric(cachetest.Round(hr), "hit-ratio")
		})
	}
}
