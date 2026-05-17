# Contributing to cache-mem

Thanks for contributing. `cache-mem` is the in-process adapter for [`github.com/ubgo/cache`](https://github.com/ubgo/cache); its correctness is defined by the shared conformance suite, so read the contract section before changing behaviour.

## Build, test, lint gate

This module has zero third-party dependencies. The full local gate, run from the module root, must be clean before any PR:

```sh
gofmt -w .                       # format (zero files reported by gofmt -l .)
go build ./...                   # must compile
go test -race -count=1 ./...     # race detector on, no flakes
golangci-lint run ./...          # must report 0 issues
```

Or via [Task](https://taskfile.dev/):

```sh
task fmt          # gofmt -w .
task test:race    # go test -race -count=2 ./...
task lint         # golangci-lint run ./...
task check        # fmt:check + vet + race tests (the pre-PR gate)
```

CI runs the same commands. A PR must be `gofmt`-clean, build, pass `go test -race ./...`, and produce **0** `golangci-lint` issues (`errcheck`, `govet`, `staticcheck`, `revive`, `gocritic`, `misspell`, `unused`, `ineffassign`, `unconvert`). The only configured exclusion is the unused `ctx` parameter, which interface compliance forces the adapter to keep.

## The conformance-suite contract

`github.com/ubgo/cache/cachetest`.`Run` **is** the `Cache` contract. `cache-mem` must keep passing it:

```go
func TestConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache { return memcache.New() })
}
```

Any change that alters observable semantics (miss returns `ErrNotFound`, TTL semantics, eviction-cause reporting, closed-cache behaviour) must keep the suite green. If the contract itself needs to change, change it in `github.com/ubgo/cache` first and update every adapter — do not special-case `cache-mem`.

## Local dev setup (the `replace` directive)

`github.com/ubgo/cache` is not yet published. `go.mod` carries:

```
replace github.com/ubgo/cache => ../cache
```

So a sibling checkout of the core repo at `../cache` is required to build and test. **Do not edit `go.mod`** (including the `replace`) in a feature PR; the replace is dropped and a real version pinned only when the core module is tagged, as a deliberate release step.

## Doc-comment style

- Every exported symbol has a doc comment that starts with its name (`revive`'s exported-comment rule is enabled — a non-conforming comment fails the lint gate).
- Document **why** and **invariants**, not just what. Algorithmic code (W-TinyLFU admission, Count-Min aging, AOF framing, atomic checkpoint rename) gets inline comments explaining the algorithm and the edge cases.
- Multi-shape fields and lock-ordering assumptions are documented at the declaration / method, not in a sidecar doc.
- Keep comments accurate if you change code; a stale invariant comment is worse than none.

## Scope rules

Do not modify `go.mod`, `LICENSE`, `NOTICE`, or `.gitignore` in a behaviour PR. `CLAUDE.md` and `TECHNICAL.md` are gitignored local context files — keep them out of commits.
