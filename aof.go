// aof.go — append-only-file durability: frame format, replay, compaction (package memcache, github.com/ubgo/cache-mem).
//
// Package role: memcache is the in-memory adapter of the ubgo/cache
// family. See doc.go for the package overview.
//
// This file: the AOF wire format and its log type. Every mutating write
// (Set/SetMulti/SetNX/Expire/Incr/Decr/Del/Flush) is fsync-appended via
// aofAppend. encodeFrame/readFrame implement the frame layout; replayAOF
// rebuilds memory on New; CompactAOF rewrites the log to one Set per live
// entry (atomic temp-file + rename) to bound growth.
//
// AI-context: the frozen wire format is length-prefixed self-contained
// gob frames — [uint32 big-endian len][gob body] per record, NOT one
// continuous gob stream. This is deliberate: independent frames make
// CompactAOF's concatenation safe and let a crash-truncated tail be
// detected as a short read (io.ErrUnexpectedEOF) and silently dropped,
// keeping every valid prefix. aofLog.append fsyncs on every write (one
// sync per write) — that throughput cost IS the point of AOF. The log
// type and aofAppend are nil-safe (no-op when WithAOF was not set).

package memcache

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Records are length-prefixed, self-contained gob frames: [uint32 len][gob].
// Independent frames (vs one continuous gob stream) make concatenation across
// CompactAOF safe and let a truncated tail from a crash be detected as a
// short read.
func encodeFrame(rec aofRec) ([]byte, error) {
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(&rec); err != nil {
		return nil, err
	}
	out := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(out, uint32(body.Len()))
	copy(out[4:], body.Bytes())
	return out, nil
}

func readFrame(r io.Reader) (aofRec, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return aofRec{}, err // io.EOF / io.ErrUnexpectedEOF = clean/truncated end
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return aofRec{}, io.ErrUnexpectedEOF // truncated tail
	}
	var rec aofRec
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&rec); err != nil {
		return aofRec{}, err
	}
	return rec, nil
}

const (
	aofSet uint8 = iota + 1
	aofDel
	aofFlush
	aofExpire
)

// aofRec is one durable write-log entry. TTL is the remaining duration at
// write time (0 = no expiry), mirroring snapshot semantics.
type aofRec struct {
	Op  uint8
	Key string
	Val []byte
	TTL time.Duration
}

// aofLog is an fsync-on-write append-only log. One sync per write trades
// throughput for near-zero data loss; that is the explicit point of AOF.
type aofLog struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

func openAOF(path string) (*aofLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &aofLog{path: path, f: f}, nil
}

func (l *aofLog) append(rec aofRec) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if frame, err := encodeFrame(rec); err == nil {
		if _, werr := l.f.Write(frame); werr == nil {
			_ = l.f.Sync()
		}
	}
}

func (l *aofLog) close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// replayAOF reads every record from path and invokes apply in order. A
// missing file is a clean cold start.
func replayAOF(path string, apply func(aofRec)) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for {
		rec, err := readFrame(f)
		if err != nil {
			return nil // EOF or truncated tail: keep what was valid
		}
		apply(rec)
	}
}

// aofAppend is the nil-safe hook the cache's write methods call.
func (c *Cache) aofAppend(rec aofRec) { c.aof.append(rec) }

// CompactAOF rewrites the log to the minimal set of records that reproduces
// the current live state (one Set per live entry), bounding unbounded growth.
// Atomic: writes a temp file then renames over the live log.
func (c *Cache) CompactAOF() error {
	if c.aof == nil {
		return nil
	}
	c.aof.mu.Lock()
	defer c.aof.mu.Unlock()

	dir := filepath.Dir(c.aof.path)
	tmp, err := os.CreateTemp(dir, ".aof-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	now := c.now()
	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			if expired(e, now) {
				continue
			}
			rec := aofRec{Op: aofSet, Key: k, Val: e.val}
			if !e.exp.IsZero() {
				rec.TTL = e.exp.Sub(now)
				if rec.TTL <= 0 {
					continue
				}
			}
			frame, ferr := encodeFrame(rec)
			if ferr == nil {
				_, ferr = tmp.Write(frame)
			}
			if ferr != nil {
				s.mu.Unlock()
				_ = tmp.Close()
				_ = os.Remove(tmpName)
				return ferr
			}
		}
		s.mu.Unlock()
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := c.aof.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, c.aof.path); err != nil {
		return err
	}
	f, err := os.OpenFile(c.aof.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	c.aof.f = f
	return nil
}
