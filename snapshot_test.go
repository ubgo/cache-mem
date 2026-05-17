// snapshot_test.go — tests for SnapshotTo / RestoreFrom and the remaining-TTL invariant.

package memcache_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	memcache "github.com/ubgo/cache-mem"
)

func TestSnapshotRestoreRoundtrip(t *testing.T) {
	ctx := context.Background()
	src := memcache.New()
	defer func() { _ = src.Close() }()
	_ = src.Set(ctx, "a", []byte("1"), 0)
	_ = src.Set(ctx, "b", []byte("2"), time.Hour)

	var buf bytes.Buffer
	if err := src.SnapshotTo(&buf); err != nil {
		t.Fatal(err)
	}

	dst := memcache.New()
	defer func() { _ = dst.Close() }()
	n, err := dst.RestoreFrom(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("restored %d, want 2", n)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		v, err := dst.Get(ctx, k)
		if err != nil || string(v) != want {
			t.Fatalf("key %s: %q %v", k, v, err)
		}
	}
}

func TestSnapshotSkipsExpiredAndPreservesRemainingTTL(t *testing.T) {
	ctx := context.Background()
	src := memcache.New()
	defer func() { _ = src.Close() }()
	_ = src.Set(ctx, "dead", []byte("x"), 30*time.Millisecond)
	_ = src.Set(ctx, "short", []byte("y"), 5*time.Second)
	time.Sleep(60 * time.Millisecond) // "dead" now expired

	var buf bytes.Buffer
	if err := src.SnapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	dst := memcache.New()
	defer func() { _ = dst.Close() }()
	if _, err := dst.RestoreFrom(&buf); err != nil {
		t.Fatal(err)
	}

	if ok, _ := dst.Has(ctx, "dead"); ok {
		t.Fatal("expired entry must not be restored")
	}
	d, err := dst.TTL(ctx, "short")
	if err != nil {
		t.Fatal(err)
	}
	// Remaining TTL preserved (relative to restore), not reset to 5s+.
	if d <= 0 || d > 5*time.Second {
		t.Fatalf("restored TTL not preserved/bounded: %v", d)
	}
}

func TestRestoreOnClosedErrors(t *testing.T) {
	c := memcache.New()
	_ = c.Close()
	if _, err := c.RestoreFrom(bytes.NewReader(nil)); err == nil {
		t.Fatal("RestoreFrom on closed cache should error")
	}
}
