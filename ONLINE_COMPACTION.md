# Online Incremental Compaction

This document describes the online incremental compaction feature added to
bbolt. Unlike the existing `Compact(dst, src)` helper and the `bbolt compact`
subcommand — which copy the whole database into a separate file and require the
source to be taken out of write service — online compaction **relocates live
pages in small batches inside the database file itself**, while the database
keeps serving reads and writes.

## Public API and CLI

- `(*DB).CompactRelocateBatch(maxPages int) (moved int, err error)`
  performs a single relocation batch (one read-write transaction). It returns
  `0` when no more pages can be moved at the moment.
- `(*DB).CompactOnline(cfg *OnlineCompactConfig) (*OnlineCompactStats, error)`
  runs relocation batches to convergence and, when safe, truncates the file.
  A `nil` config uses defaults. See `OnlineCompactConfig` for knobs:
  `MaxPagesPerBatch`, `MaxBatches`, `ShrinkTimeout`, `DisableShrink`.
- The `bbolt compact-online <file>` CLI subcommand wraps `CompactOnline`.
  It takes the same exclusive file lock as any writer; the *in-process* API is
  the entry point intended for databases that stay in service.
- `Compact(dst, src)` and `bbolt compact` are unchanged and still supported.

The on-disk format is **not changed**. Old bbolt versions can open a file that
was produced — or partially compacted — by this feature without special flags.

## Page migration

Each batch is an ordinary write transaction:

1. Build the **live page graph** from the current meta snapshot: traverse the
   top-level bucket and every (nested) bucket's branch/leaf pages. Overflow
   runs (large values) are tracked as multi-page units. Meta pages (0/1) and
   the freelist page are *not* data units.
2. Build a model of the currently **available** free spans from the freelist.
3. Walk live units from the highest page id downward. For each unit, reserve a
   free destination strictly below its current id, plus destinations for every
   page on its ancestor chain up to the bucket root. Reservations use the same
   deterministic lowest-fit policy as the freelist backends
   (`array.AllocateInRange` / `hashMap.AllocateInRange`), so planned
   destinations match the actual allocations exactly. A unit whose whole chain
   cannot fit is skipped; later, smaller units are still attempted.
4. For every planned page, allocate a fresh dirty page at the destination
   (`TakeFreeSpan`), copy the bytes verbatim, and rewrite outgoing references:
   - branch elements' child `pgid`,
   - leaf bucket-entry headers (`InBucket.root`) for non-inline sub-buckets.
   The top-level bucket root is updated in `tx.meta` / `tx.root`.
5. Free every old span through the regular `freelist.Free(txid, page)` path.
6. Commit via the standard `Tx.Commit` flow, including the usual freelist
   serialization and meta fsync.

Because every changed page is copied to a new location and old pages are freed
through the freelist rather than overwritten, the operation composes with the
existing copy-on-write machinery.

## Snapshot visibility

bbolt never reuses a page that an open read-only transaction might still read:
freed pages enter the freelist as *pending* for the freeing txid and are only
made available once all older read transactions have closed
(`ReleasePendingPages` / read-txid tracking). Online compaction relies on this
exact mechanism — it does not introduce a new page-reuse path:

- old readers keep faulting in the untouched old pages from the mmap;
- new transactions see the relocated pages via the freshly committed meta.

Consequently, if a long-lived snapshot is still pinning the pages that a batch
would need as destinations or would vacate, that batch simply makes no progress
and `CompactOnline` returns; callers can retry after the long transaction ends.

## Freelist

- The **freelist page itself is never relocated as a data page.** Every commit
  already frees the old freelist page and writes a new one (`commitFreelist`),
  so it naturally moves toward lower page ids over successive batches. This
  also satisfies the requirement that references inside the freelist's own
  pages stay consistent — those pages are rebuilt, never patched.
- Both freelist backends are supported and tested:
  - `array` — contiguous block scan (`array.go`),
  - `hashmap` — span maps (`hashmap.go`).
  New internal primitives (`AllocateInRange`, `TakeFreeSpan`, `RemoveFreeIDs`,
  `FreePageIDs`, `PendingPageIDs`) are implemented for both.
- The freelist's **on-disk serialization format is unchanged** (the same
  array/overflow encoding in shared `Write`/`Read`), preserving compatibility
  with older versions.

## Mmap interaction and file shrink

Relocation batches never change the high-water mark, so they require no remap.

Physical space is returned by a separate **shrink transaction**:

1. Start a writer (which first releases pending pages of closed readers).
2. Compute the highest live page and the longest contiguous free suffix; bump
   the suffix start past any pending page still pinned by an open reader.
3. Reserve room for the new freelist strictly below the new high-water mark
   (either in an existing free hole or at the head of the suffix), remove every
   free id at/above the new high-water mark from the freelist, lower
   `meta.pgid`, and commit + fsync normally.
4. Only after the new meta is durable, `ftruncate` the file and remap.

The remap takes `mmaplock` for writing; read transactions hold it in read mode
for their entire lifetime, exactly as they do for growth-time remaps. The
shrink waits, bounded by `ShrinkTimeout`, for open readers to drain rather than
unmapping memory they might fault in (preventing dangling pointers / SIGBUS).
On Windows the file is truncated in place while keeping the strictly-larger
existing mapping, because the platform mmap re-creates the file at the rounded
mapping size.

## Crash recovery

There is no persistent "compaction in progress" marker, so there is no
half-compacted state that could later be mistaken for a valid database:

- A relocation batch is one normal transaction — atomic via the alternating
  meta pages. A crash before the meta fsync leaves the previous meta and the
  old pages valid; pages written past it are harmless garbage.
- For shrink, the ordering is deliberate: **meta first (durable), truncate
  second.**
  - crash before the meta fsync → old meta, old (larger) file: valid;
  - crash after the meta fsync but before/within truncate → new meta points
    only within the file; the file is merely larger than required, the extra
    tail is not referenced and is reused on future growth: valid;
  - crash after truncate → fully shrunk: valid.
- Because the freelist is pruned of every id ≥ new high-water mark before the
  meta is written, no committed metadata can reference a page beyond EOF after
  a crash.

Failpoints used by the crash tests: `beforeSyncDataPages`, `shrinkFileError`,
and `afterShrinkCommit` (the last simulates a kill in the
meta-durable-but-not-truncated window). Tests reopen the database and run
`Tx.Check` plus key/value verification after each injected failure.

## Testing

- `online_compact_test.go`: convergence, shrink ratio, reopen + `Tx.Check`,
  concurrent reads during batches, a long read-only transaction that must keep
  its snapshot and block the shrink until it closes, nested buckets, and
  overflow (large-value) pages.
- `internal/freelist/online_compact_test.go`: new freelist primitives for both
  backends.
- `tests/failpoint/online_compact_failpoint_test.go`: fsync failure, truncate
  failure, and a subprocess-simulated crash in the shrink commit window, each
  followed by reopen + check.
- `cmd/bbolt/command/command_compact_online_test.go`: CLI behavior.
