package memcache_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	memcache "github.com/ubgo/cache-mem"
)

func TestPeriodicCheckpointAndRestore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.ckpt")

	c := memcache.New(memcache.WithCheckpoint(path, 25*time.Millisecond))
	_ = c.Set(ctx, "k1", []byte("v1"), 0)
	_ = c.Set(ctx, "k2", []byte("v2"), time.Hour)

	// Wait for at least one periodic checkpoint to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		probe := memcache.New()
		n, _ := probe.RestoreFromFile(path)
		_ = probe.Close()
		if n == 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	_ = c.Close() // also writes a final checkpoint

	restored := memcache.New()
	defer func() { _ = restored.Close() }()
	n, err := restored.RestoreFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("restored %d entries, want 2", n)
	}
	if v, err := restored.Get(ctx, "k1"); err != nil || string(v) != "v1" {
		t.Fatalf("k1 not restored: %q %v", v, err)
	}
}

func TestRestoreFromMissingFileIsColdStart(t *testing.T) {
	c := memcache.New()
	defer func() { _ = c.Close() }()
	n, err := c.RestoreFromFile(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil || n != 0 {
		t.Fatalf("missing checkpoint should be a clean cold start, got %d %v", n, err)
	}
}
