// aof_test.go — tests for append-only-file durability: append, replay, truncated-tail recovery, and CompactAOF.

package memcache_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	memcache "github.com/ubgo/cache-mem"
)

func TestAOFReplayAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.aof")

	c1 := memcache.New(memcache.WithAOF(path))
	_ = c1.Set(ctx, "a", []byte("1"), 0)
	_ = c1.Set(ctx, "b", []byte("2"), time.Hour)
	_, _ = c1.Incr(ctx, "ctr", 5)
	_ = c1.Del(ctx, "b")
	_ = c1.Set(ctx, "c", []byte("3"), 0)
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": a fresh cache pointed at the same AOF must reconstruct state.
	c2 := memcache.New(memcache.WithAOF(path))
	defer func() { _ = c2.Close() }()

	if v, err := c2.Get(ctx, "a"); err != nil || string(v) != "1" {
		t.Fatalf("a not recovered: %q %v", v, err)
	}
	if v, err := c2.Get(ctx, "c"); err != nil || string(v) != "3" {
		t.Fatalf("c not recovered: %q %v", v, err)
	}
	if has, _ := c2.Has(ctx, "b"); has {
		t.Fatal("b was deleted before restart; must not reappear")
	}
	if n, _ := c2.Incr(ctx, "ctr", 0); n != 5 {
		t.Fatalf("counter not recovered: got %d want 5", n)
	}
}

func TestAOFFlushClearsReplayedState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "f.aof")

	c1 := memcache.New(memcache.WithAOF(path))
	_ = c1.Set(ctx, "x", []byte("v"), 0)
	_ = c1.Flush(ctx)
	_ = c1.Set(ctx, "y", []byte("w"), 0)
	_ = c1.Close()

	c2 := memcache.New(memcache.WithAOF(path))
	defer func() { _ = c2.Close() }()
	if has, _ := c2.Has(ctx, "x"); has {
		t.Fatal("x set before Flush must not survive replay")
	}
	if v, _ := c2.Get(ctx, "y"); string(v) != "w" {
		t.Fatalf("y written after Flush must survive: %q", v)
	}
}

func TestCompactAOFPreservesState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "c.aof")

	c1 := memcache.New(memcache.WithAOF(path))
	for i := 0; i < 100; i++ {
		_ = c1.Set(ctx, "k", []byte("old"), 0) // many rewrites of one key
	}
	_ = c1.Set(ctx, "k", []byte("final"), 0)
	_ = c1.Set(ctx, "keep", []byte("yes"), 0)

	fi1 := fileSize(t, path)
	if err := c1.CompactAOF(); err != nil {
		t.Fatal(err)
	}
	fi2 := fileSize(t, path)
	if fi2 >= fi1 {
		t.Fatalf("compaction did not shrink the log (%d -> %d)", fi1, fi2)
	}
	_ = c1.Set(ctx, "after", []byte("z"), 0) // appends still work post-compact
	_ = c1.Close()

	c2 := memcache.New(memcache.WithAOF(path))
	defer func() { _ = c2.Close() }()
	for k, want := range map[string]string{"k": "final", "keep": "yes", "after": "z"} {
		if v, err := c2.Get(ctx, k); err != nil || string(v) != want {
			t.Fatalf("post-compact %s: %q %v (want %q)", k, v, err, want)
		}
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}
