# Online incremental compaction

`DB.OnlineCompact` (CLI: `bbolt online-compact`) reclaims fragmented file space
**while the database keeps serving reads and writes**. The existing
`Compact(dst, src)` (CLI: `bbolt compact`) is an offline full rewrite and is
unchanged.

## Why full rewrite is not used

`Compact` copies every key/value into a brand-new database file. That requires
the source to be effectively quiescent, rewrites the whole dataset, and only
pays off when the rewrite finishes. Online compaction instead moves existing
**pages** to already-free lower page IDs, one small batch at a time, and finally
truncates the free file tail. Every batch is an ordinary bbolt read-write
transaction, so the whole machinery of copy-on-write, freelist and crash
recovery is reused; there is no new on-disk format.

## Page migration

1. Enumerate live pages directly from the mmap snapshot (read-only transaction,
   nodes are never materialized): branch and leaf pages (with overflow
   continuations) of every bucket, top-level or nested. Meta pages 0/1 and the
   freelist page are excluded. Pages are grouped into contiguous runs
   ("spans") and ordered by descending start page ID.
2. Pick up to `BatchPages` worth of the highest spans. In a single read-write
   transaction, open each owning bucket and `Cursor.Seek` to the span's first
   key, then call `Cursor.node()`. This materializes exactly the target leaf
   page together with **all ancestor branch nodes** (and, for nested buckets,
   the parent leaf that stores the child bucket header).
3. `Tx.Commit` runs the normal rebalance/spill path. Every materialized node is
   copied to a newly allocated page; `node.spill` inserts the new child pgids
   into parent inodes, and `Bucket.spill` rewrites child bucket-root pointers
   stored in parent leaf entries. Therefore **every pgid reference to a moved
   page is fixed by the existing COW logic** — including pointers from parent
   branches, parent bucket leaves and bucket headers.
4. All allocations of a relocation transaction are bounded
   (`freelist.AllocateBelow`) so every new page is strictly below the lowest
   source page of the batch; moved pages therefore drift monotonically toward
   the file head and the high-address tail becomes free. If the lower free
   space is too fragmented for the batch, it is retried with fewer spans.

Because relocation is plain COW, pages freed by the commit are only put on the
**pending** list and are not reused until every read transaction that might see
them has closed. This is the same guarantee that protects readers during normal
writes.

## Snapshot visibility (hard constraint 1)

Read transactions keep the meta snapshot and the mmap pages captured at `Begin`.
A moved page is never overwritten in place: its copy gets a new pgid and the old
page enters the freelist pending list. It is reused only after the transaction
that freed it is older than the oldest open reader
(`ReleasePendingPages` / `release` in `internal/freelist`). A long reader can
therefore keep traversing its old pages for its whole lifetime, regardless of
how much compaction runs concurrently (see
`TestOnlineCompact_ConcurrentLongReader`).

## Freelist and mmap/shrink interaction (constraints 2–4)

- **Freelist backends.** Both implementations, `array` and `hashmap`, gained:
  - `AllocateBelow(txid, n, below)` — allocate a contiguous block whose pages
    are all below a boundary (array reuses its ordered scan; hashmap scans its
    span map for the lowest fitting start).
  - `DropAbove(pgid)` — discard free and pending pages at/above a new high water
    mark during shrink.
  - `ReadonlyTxIDs()` — list of currently open readers used to decide when the
    physical truncate is safe.
- **Freelist page itself.** It is never migrated as data. Each commit allocates
  a fresh freelist page (serialized in the existing, unchanged format) and frees
  the old one. During shrink the new freelist page is allocated **below** the
  new high water mark; if it cannot fit (e.g. long readers still hold those
  pages pending) the shrink is deferred rather than extending the file.
- **On-disk format.** Nothing changes: meta, freelist, branch and leaf pages use
  the historical serialization. Old bbolt versions open a compacted file with
  no special handling. The only visible difference is a shorter file and a
  lower `meta.pgid`, both of which are ordinary states.
- **Physical shrink.** The final transaction lowers `meta.pgid` and fsyncs the
  meta **before** the file is `Truncate`d, so the durable state always points
  inside the truncated region. `Truncate` runs while holding the writer lock
  *and* the mmap lock exclusively, and only after all readers that started
  before the shrink transaction have closed. The mapping is **not** remapped:
  shortening the file leaves all live (lower) addresses valid, so a reader can
  never touch an unmapped page or receive SIGBUS. The Windows mmap path is also
  safe because no remap to a smaller size is performed.

## Crash recovery (constraint 5)

- During relocation, each batch is a standard transaction: either its meta is
  fully on disk or the previous meta remains active. Killing the process or
  losing power at any fsync/commit point leaves a normal database at the last
  committed txid; rerunning `OnlineCompact` resumes from there. There is no
  in-memory "compaction in progress" marker and no half-compacted special state.
- During shrink the ordering is: write+sync data/freelist pages → write+sync
  meta (new, smaller pgid) → truncate+sync file.
  - crash before the meta sync: nothing changed, file still large;
  - crash after meta sync but before/within truncate: the database is valid and
    already logically compacted; its trailing pages are simply unreachable free
    space on disk and are truncated by the next run;
  - crash after truncate: fully compacted database.
  Failpoints `beforeOnlineCompactShrinkTruncate`,
  `beforeOnlineCompactShrinkSync` and `beforeWriteMetaError`, plus a
  panic/kill test, exercise these points (`tests/failpoint`).

## API and CLI

- `db.OnlineCompact(*OnlineCompactOptions) (OnlineCompactStats, error)`
  - `BatchPages`: tail pages relocated per transaction (default 64).
  - `ShrinkTimeout`: how long to wait for old readers before truncating; a
    negative value performs relocation only and defers truncation.
  - Returns `ErrCompactionShrinkBlocked` when relocation completed but readers
    still prevent truncation; rerun later.
- CLI: `bbolt online-compact [--batch-pages N] [--shrink-timeout D] <file>`.

## Limits (honest coverage)

Like every copy-free compactor, a live span larger than every free hole below
it cannot be moved (there is nowhere to put it). In that case `OnlineCompact`
moves everything that fits and truncates only the free tail above the highest
live page; a span wedged at the tail bounds how much space is reclaimed without
a full offline rewrite. The normal case — free pages interleaved with live ones
after deletes/updates — reclaims nearly the entire tail.
