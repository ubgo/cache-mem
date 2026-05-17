package memcache

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ubgo/cache"
)

// snapEntry is the on-disk representation. TTL is stored as the *remaining*
// duration (0 = no expiry) so a restored entry expires relative to restore
// time, never living past its original deadline.
type snapEntry struct {
	Key string
	Val []byte
	TTL time.Duration // 0 = no expiry; >0 = remaining at snapshot time
}

// SnapshotTo writes a gob stream of all live (non-expired) entries to w. Use
// it on shutdown for a warm restart:
//
//	f, _ := os.Create("cache.snap"); defer f.Close()
//	_ = c.SnapshotTo(f)
//
// Snapshotting takes each shard lock briefly; concurrent ops are safe.
func (c *Cache) SnapshotTo(w io.Writer) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	return c.snapshotTo(w)
}

// snapshotTo is the guard-free implementation, usable during Close.
func (c *Cache) snapshotTo(w io.Writer) error {
	enc := gob.NewEncoder(w)
	now := c.now()
	for _, s := range c.shards {
		s.mu.Lock()
		batch := make([]snapEntry, 0, len(s.items))
		for k, e := range s.items {
			if expired(e, now) {
				continue
			}
			se := snapEntry{Key: k, Val: e.val}
			if !e.exp.IsZero() {
				se.TTL = e.exp.Sub(now)
				if se.TTL <= 0 {
					continue // expired between scan and now
				}
			}
			batch = append(batch, se)
		}
		s.mu.Unlock()
		for i := range batch {
			if err := enc.Encode(&batch[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// RestoreFrom loads a stream written by SnapshotTo. Entries whose remaining
// TTL has already elapsed are skipped. Existing keys are overwritten. Returns
// the number of entries restored.
func (c *Cache) RestoreFrom(r io.Reader) (int, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	dec := gob.NewDecoder(r)
	n := 0
	for {
		var se snapEntry
		err := dec.Decode(&se)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if se.TTL < 0 {
			continue // already expired
		}
		s := c.shardFor(se.Key)
		s.mu.Lock()
		c.store(s, se.Key, se.Val, se.TTL)
		s.mu.Unlock()
		c.sets.Add(1)
		n++
	}
}

// writeCheckpoint atomically snapshots to cfg.ckptPath (temp file + rename so
// a crash mid-write never leaves a truncated checkpoint). Guard-free.
func (c *Cache) writeCheckpoint() error {
	path := c.cfg.ckptPath
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ckpt-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if serr := c.snapshotTo(tmp); serr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return serr
	}
	if cerr := tmp.Close(); cerr != nil {
		_ = os.Remove(tmpName)
		return cerr
	}
	return os.Rename(tmpName, path)
}

// checkpointLoop periodically writes a checkpoint until Close.
func (c *Cache) checkpointLoop() {
	t := time.NewTicker(c.cfg.ckptInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			_ = c.writeCheckpoint()
		}
	}
}

// RestoreFromFile loads a checkpoint/snapshot file written by SnapshotTo or
// WithCheckpoint. Missing file is not an error (cold start) — it returns
// (0, nil). Returns the number of entries restored.
func (c *Cache) RestoreFromFile(path string) (int, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	return c.RestoreFrom(f)
}
