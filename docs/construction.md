# Construction & lifecycle

### New

`func New(opts ...Option) *Cache`

What it is: the sole constructor. Builds a sharded in-memory cache, replays an
existing AOF (if `WithAOF`), and starts the background sweeper / checkpoint
goroutines as configured.

Use cases:

- A fast process-local cache with no external dependency.
- The L1 tier of a [`cache-tiered`](https://github.com/ubgo/cache-tiered) stack.
- A test double that conforms to `cache.Cache` without Docker.

```go
package main

import (
	"context"
	"fmt"

	memcache "github.com/ubgo/cache-mem"
)

func main() {
	c := memcache.New(
		memcache.WithMaxEntries(100_000),
		memcache.WithMaxBytes(256<<20),
	)
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "user:42", []byte("alice"), 0)
	v, _ := c.Get(ctx, "user:42")
	fmt.Println(string(v)) // alice
}
```

### Cache

`type Cache struct { ... }`

What it is: the sharded in-memory adapter returned by `New`. Every public
method acquires the owning shard's mutex; whole-cache operations lock each
shard briefly and serially. After `Close` every method returns
`cache.ErrClosed`.

Use cases:

- Hold as a `cache.Cache` interface value so backends are swappable.
- Hold as the concrete `*memcache.Cache` when you need `SnapshotTo` /
  `RestoreFrom` / `CompactAOF`, which are not on the interface.

```go
import (
	"github.com/ubgo/cache"
	memcache "github.com/ubgo/cache-mem"
)

// Swappable as the interface...
var generic cache.Cache = memcache.New()

// ...or concrete when you need persistence extras.
concrete := memcache.New(memcache.WithAOF("cache.aof"))
_ = concrete.CompactAOF()
```

### Close

`func (c *Cache) Close() error`

What it is: idempotent shutdown. The first call flips the closed guard, writes
a final checkpoint (if configured), stops the sweeper/checkpoint goroutines,
and closes the AOF handle. Subsequent calls are no-ops.

Use cases:

- `defer c.Close()` so the final checkpoint is written and goroutines stop.
- Graceful shutdown handlers — safe to call from multiple paths (idempotent).

```go
c := memcache.New(memcache.WithCheckpoint("cache.snap", 30*time.Second))
defer c.Close() // writes a final checkpoint, then stops loops
```

### Ping

`func (c *Cache) Ping(ctx context.Context) error`

What it is: a liveness probe. Returns `nil` while open, `cache.ErrClosed`
after `Close`. An in-memory cache has no network, so it never fails for any
other reason.

Use cases:

- Uniform health checks across backends behind the `cache.Cache` interface.
- Asserting a cache has not been closed by another component.

```go
if err := c.Ping(ctx); err != nil {
	log.Fatal("cache unavailable:", err)
}
```
