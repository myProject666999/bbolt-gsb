package bbolt

import (
	"errors"
	"fmt"
	"sort"
	"time"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

// DefaultOnlineCompactBatchPages is the default upper bound on the number of
// file-tail pages relocated in a single online compaction transaction.
const DefaultOnlineCompactBatchPages = 64

// OnlineCompactOptions controls an online incremental compaction run.
type OnlineCompactOptions struct {
	// BatchPages limits the number of high-address ("tail") live pages that
	// are relocated in a single short-lived write transaction. Smaller
	// batches reduce the write-lock hold time and the amount of work that
	// must be redone on failure; zero means DefaultOnlineCompactBatchPages.
	BatchPages int

	// ShrinkTimeout is the maximum time OnlineCompact waits for long-lived
	// read transactions to finish before physically shrinking the file.
	// Reclaiming the file tail requires exclusive access to the mmap, so all
	// read transactions that started before the final shrink commit must be
	// closed. Zero means a short default timeout (30s). A negative value
	// disables waiting: the logical compaction (page relocation) is still
	// performed and the file is shrunk on the next run once the readers are
	// gone.
	ShrinkTimeout time.Duration
}

// OnlineCompactStats summarizes an online incremental compaction run.
type OnlineCompactStats struct {
	// Batches is the number of committed relocation transactions.
	Batches int
	// MovedPages is the number of live pages relocated to lower page IDs.
	MovedPages int
	// HighWaterBefore is the high water mark (allocated page count) before
	// the run.
	HighWaterBefore common.Pgid
	// HighWaterAfter is the high water mark after the run.
	HighWaterAfter common.Pgid
	// Shrunk reports whether the run physically truncated the data file.
	Shrunk bool
	// ShrinkDeferred reports whether page relocation completed but the file
	// could not be truncated because old read transactions were still open.
	// A later OnlineCompact run will perform the truncation once they close.
	ShrinkDeferred bool
}

// ErrCompactionShrinkBlocked is returned when logical compaction succeeded but
// the file could not be shrunk because long-lived read transactions were still
// open when ShrinkTimeout elapsed.
var ErrCompactionShrinkBlocked = errors.New("online compact: shrink blocked by long-lived read transactions; rerun OnlineCompact after they close")

// ErrCompactionRetryable is returned when an online compaction run makes no
// structural change because it raced with concurrent writers (e.g. a bucket
// present in the relocation snapshot was removed before its relocation
// transaction committed). The whole run can safely be retried.
var ErrCompactionRetryable = errors.New("online compact: concurrent modification, retry")

// errCompactNoSpace is an unexported sentinel indicating that a relocation
// batch could not place a page below its current page ID. The batch is rolled
// back and retried with fewer pages.
var errCompactNoSpace = errors.New("online compact: no free space below relocated page")

// OnlineCompact incrementally relocates the highest-address live pages of the
// database into already-free lower pages and, once no readers hold old
// snapshots, truncates the free file tail. Unlike Compact, it does not copy
// the database and does not require exclusive/offline access: every relocation
// step is a normal, short-lived read-write transaction, so concurrent readers
// and writers are served continuously.
//
// The existing offline Compact(dst, src) function is unchanged.
func (db *DB) OnlineCompact(options *OnlineCompactOptions) (OnlineCompactStats, error) {
	if options == nil {
		options = &OnlineCompactOptions{}
	}
	batchPages := options.BatchPages
	if batchPages <= 0 {
		batchPages = DefaultOnlineCompactBatchPages
	}
	shrinkTimeout := options.ShrinkTimeout
	if shrinkTimeout == 0 {
		shrinkTimeout = 30 * time.Second
	}

	var stats OnlineCompactStats

	// Relocate tail live pages batch by batch. Each iteration is a fully
	// independent read-write transaction; a crash or a failed commit at any
	// point simply leaves the database in the state of the last committed
	// transaction, which is always a valid, consistent bbolt database.
	//
	// When the database is being written concurrently, a batch may observe a
	// page layout that has already changed and therefore relocate nothing
	// (ErrCompactionRetryable). A bounded number of such empty batches is
	// tolerated; beyond that the run returns ErrCompactionRetryable to the
	// caller instead of spinning behind the writer lock.
	const maxIdleBatches = 50
	idleBatches := 0
	for {
		relocated, moved, done, err := db.compactRelocateBatch(batchPages)
		if err != nil {
			if errors.Is(err, ErrCompactionRetryable) {
				idleBatches++
				if idleBatches > maxIdleBatches {
					return stats, ErrCompactionRetryable
				}
				continue
			}
			return stats, err
		}
		idleBatches = 0
		if relocated {
			stats.Batches++
			stats.MovedPages += moved
		}
		if done {
			break
		}
		if !relocated {
			// compactRelocateBatch guarantees forward progress or termination;
			// treat any other no-progress result as a retryable race.
			return stats, ErrCompactionRetryable
		}
	}

	// Compute the resulting high water mark and physically shrink the file.
	oldHWM, newHWM, freeTail, err := db.compactTailInfo()
	if err != nil {
		return stats, err
	}
	stats.HighWaterBefore = oldHWM
	stats.HighWaterAfter = newHWM

	if freeTail == 0 {
		// Nothing to reclaim (e.g. densely packed database). The logical
		// relocation is complete and the next deletion will create room.
		return stats, nil
	}

	// Wait for readers that could still observe the pre-shrink snapshot. The
	// shrink commit itself takes a fresh txid, so any reader with a strictly
	// smaller txid must be gone before the file tail is truncated.
	if shrinkTimeout >= 0 {
		if err := db.waitForReadersBefore(shrinkTimeout); err != nil {
			stats.ShrinkDeferred = true
			return stats, ErrCompactionShrinkBlocked
		}
	} else {
		// Non-blocking mode: if any read transaction is still open, defer the
		// whole shrink (including the logical high water mark commit) to a
		// later run.
		db.metalock.Lock()
		ids := db.freelist.ReadonlyTxIDs()
		db.metalock.Unlock()
		if len(ids) > 0 {
			stats.ShrinkDeferred = true
			return stats, ErrCompactionShrinkBlocked
		}
	}

	shrunk, err := db.compactShrink()
	if err != nil {
		if errors.Is(err, ErrCompactionShrinkBlocked) {
			stats.ShrinkDeferred = true
			return stats, err
		}
		return stats, err
	}
	stats.Shrunk = shrunk
	if !shrunk {
		stats.ShrinkDeferred = true
	}
	return stats, nil
}

// waitForReadersBefore blocks until there are no open read transactions (any
// open transaction necessarily started before the upcoming shrink commit) or
// until timeout elapses.
func (db *DB) waitForReadersBefore(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		db.metalock.Lock()
		ids := db.freelist.ReadonlyTxIDs()
		db.metalock.Unlock()
		if len(ids) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrCompactionShrinkBlocked
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// compactLiveSpan describes one contiguous run of live (reachable) data
// pages belonging to a bucket.
type compactLiveSpan struct {
	start common.Pgid
	size  int // number of contiguous pages (includes overflow pages)
	// bucketPath is the sequence of top-level/sub-bucket keys required to
	// open the bucket that owns the pages.
	bucketPath [][]byte
	// firstKey is a key that lives on (or below) the first page of the span.
	// Seeking to it materializes the exact page together with all ancestors.
	firstKey []byte
}

// compactRelocateBatch performs one relocation transaction.
//
// It enumerates the currently live page spans, takes up to batchPages worth of
// the highest-address spans and rewrites the owning nodes through the regular
// copy-on-write spill machinery, forcing all allocations below the lowest
// source page ID of the selected spans. If the lower-address free space is
// too fragmented to place the whole batch the transaction is rolled back and
// retried with fewer spans. If even the single highest span cannot be moved
// below itself then no further tail reclaiming is possible: (relocated=false,
// done=true) is returned and the caller shrinks only the free tail above the
// highest live page (this is the inherent limit of copy-free compaction: a
// page larger than any lower free hole cannot be relocated without copying the
// whole database).
func (db *DB) compactRelocateBatch(batchPages int) (relocated bool, moved int, done bool, err error) {
	spans, hwm, err := db.enumerateLiveSpans()
	if err != nil {
		return false, 0, false, err
	}
	if hwm <= 2 || len(spans) == 0 {
		return false, 0, true, nil
	}

	// Fast path: the highest live page already abuts the high water mark.
	top := spans[0]
	if top.start+common.Pgid(top.size) == hwm {
		return false, 0, true, nil
	}

	// Select up to batchPages worth of highest spans.
	pages := 0
	n := 0
	for _, sp := range spans {
		if n > 0 && pages >= batchPages {
			break
		}
		pages += sp.size
		n++
	}

	// Retry with progressively smaller batches. All allocations of a batch are
	// bounded by the lowest selected source span so every moved page strictly
	// descends and progress is monotonic.
	window := n
	for window >= 1 {
		batch := spans[:window]
		limit := batch[0].start
		for _, sp := range batch[1:] {
			if sp.start < limit {
				limit = sp.start
			}
		}

		movedNow, commitErr := db.relocateSpans(batch, limit)
		if commitErr == nil {
			return true, movedNow, false, nil
		}
		if !errors.Is(commitErr, errCompactNoSpace) {
			return false, 0, false, commitErr
		}
		window--
	}

	// The single highest span cannot be placed below itself.
	return false, 0, true, nil
}

// relocateSpans materializes and spills the given spans within a single
// read-write transaction whose allocations are bounded by limit.
func (db *DB) relocateSpans(spans []compactLiveSpan, limit common.Pgid) (moved int, err error) {
	tx, err := db.Begin(true)
	if err != nil {
		return 0, err
	}
	tx.compactAllocLimit = limit

	rollback := func() {
		tx.compactAllocLimit = 0
		_ = tx.Rollback()
	}

	// Open each owning bucket (opening materializes ancestor leaf pages of
	// parent buckets lazily as needed) and seek to the span's first key. The
	// seek materializes the exact source page together with all of its node
	// ancestors. On commit, the normal copy-on-write spill rewrites every
	// materialized node, fixes every parent pgid reference (including the
	// bucket-root pointers stored in parent leaf entries) and allocates only
	// below limit.
	//
	// A span is skipped if concurrent writers changed the layout between the
	// enumeration snapshot and this transaction (its first key now lives on a
	// different page, or the owning bucket is gone). Skipped spans are simply
	// reconsidered by the next batch; the commit is still useful as long as at
	// least one span was materialized.
	skipped := 0
	for _, sp := range spans {
		b := openBucketPath(tx, sp.bucketPath)
		if b == nil {
			skipped++
			continue
		}
		b.FillPercent = 1.0
		c := b.Cursor()
		c.Seek(sp.firstKey)
		// Verify the traversal stack still contains the page recorded in the
		// enumeration snapshot (the terminal frame for a leaf span, any
		// ancestor frame for a branch span). If concurrent writers moved it,
		// skip the span and reconsider it in a later batch.
		if !cursorStackContainsPage(c, sp.start) {
			skipped++
			continue
		}
		// Seek only reads the mmap pages; calling node() materializes the
		// terminal leaf together with every ancestor branch node into the
		// bucket's node cache so Commit's spill rewrites them to new pages.
		// A branch span is on the stack as an ancestor, and it is materialized
		// transitively while node() walks down from the root.
		c.node()
		moved += sp.size
	}

	if moved == 0 {
		// Every selected span raced with a writer; nothing to commit.
		rollback()
		return 0, ErrCompactionRetryable
	}

	// gofail: var beforeOnlineCompactRelocateCommit struct{}
	if err := tx.Commit(); err != nil {
		rollback()
		return 0, err
	}
	return moved, nil
}

// openBucketPath opens a (possibly nested) bucket by its key path in tx. The
// root bucket is represented by an empty path.
func openBucketPath(tx *Tx, path [][]byte) *Bucket {
	if len(path) == 0 {
		return &tx.root
	}
	b := tx.Bucket(path[0])
	if b == nil {
		return nil
	}
	for _, key := range path[1:] {
		b = b.Bucket(key)
		if b == nil {
			return nil
		}
	}
	return b
}

// enumerateLiveSpans returns every contiguous run of live data pages in the
// database (branch, leaf and leaf overflow pages of all buckets, including
// inline buckets' containing leaf page only once) sorted by descending start
// page ID. Meta pages and freelist pages are excluded. The enumeration runs in
// a read-only transaction and reads the mmap pages directly without
// materializing nodes.
func (db *DB) enumerateLiveSpans() ([]compactLiveSpan, common.Pgid, error) {
	var spans []compactLiveSpan
	var hwm common.Pgid
	err := db.View(func(tx *Tx) error {
		hwm = tx.meta.Pgid()
		spans = enumerateLiveSpansFromTx(tx)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return spans, hwm, nil
}

// enumerateLiveSpansFromTx collects all live page spans visible to tx (sorted
// by descending start page ID). It reads mmap pages directly and never
// materializes nodes, so it is safe to call on a read-write transaction
// without turning the live pages into dirty pages.
func enumerateLiveSpansFromTx(tx *Tx) []compactLiveSpan {
	var spans []compactLiveSpan
	visited := make(map[common.Pgid]struct{})
	// Walk the root bucket and, for each leaf page, discover both data pages
	// and nested buckets via their leaf entries.
	_ = enumerateBucketPages(tx, nil, tx.meta.RootBucket().RootPage(), &spans, visited)
	// Sort descending by start page ID (stable for deterministic batches).
	sortSpansDesc(spans)
	return spans
}

// enumerateBucketPages recursively collects page spans of one bucket tree and
// of every nested bucket discovered on its leaf pages.
func enumerateBucketPages(tx *Tx, path [][]byte, rootPgid common.Pgid, out *[]compactLiveSpan, visited map[common.Pgid]struct{}) error {
	var walkPage func(pgid common.Pgid, currentPath [][]byte) error
	walkPage = func(pgid common.Pgid, currentPath [][]byte) error {
		if pgid < 2 {
			return nil
		}
		if _, ok := visited[pgid]; ok {
			return nil
		}

		p := tx.page(pgid)
		size := 1 + int(p.Overflow())
		// Mark the whole contiguous run (overflow pages follow the start
		// page); otherwise an overflow continuation would be parsed as a
		// standalone leaf/branch page and recurse into an ancestor.
		for j := 0; j < size; j++ {
			visited[pgid+common.Pgid(j)] = struct{}{}
		}
		*out = append(*out, compactLiveSpan{
			start:      pgid,
			size:       size,
			bucketPath: append([][]byte(nil), currentPath...),
			firstKey:   firstKeyOfPage(tx, p),
		})

		if p.IsBranchPage() {
			for i := 0; i < int(p.Count()); i++ {
				if err := walkPage(p.BranchPageElement(uint16(i)).Pgid(), currentPath); err != nil {
					return err
				}
			}
			return nil
		}

		if !p.IsLeafPage() {
			return nil
		}

		// Discover nested buckets: each bucket leaf entry embeds an InBucket
		// header; non-inline buckets have their own root page tree.
		for i := 0; i < int(p.Count()); i++ {
			elem := p.LeafPageElement(uint16(i))
			if (elem.Flags() & common.BucketLeafFlag) == 0 {
				continue
			}
			value := elem.Value()
			if len(value) < common.BucketHeaderSize {
				continue
			}
			inb := (*common.InBucket)(unsafe.Pointer(&value[0]))
			childRoot := inb.RootPage()
			if childRoot == 0 {
				// Inline bucket: its content lives within this very leaf
				// page, which is already recorded. Nothing else to walk.
				continue
			}
			childPath := append(append([][]byte(nil), currentPath...), elem.Key())
			if err := walkPage(childRoot, childPath); err != nil {
				return err
			}
		}
		return nil
	}

	return walkPage(rootPgid, path)
}

// firstKeyOfPage returns a key guaranteed to materialize the page during a
// cursor Seek: for a leaf page it is its own first key; for a branch page it
// is the key of its first branch element (the separator of its leftmost
// child subtree). Seeking that separator walks through the branch page itself.
// Note that bbolt does not order page IDs within a branch, so following
// leftmost-child pointers to a leaf would risk traversing a pointer cycle.
func firstKeyOfPage(tx *Tx, p *common.Page) []byte {
	if p.IsBranchPage() && p.Count() > 0 {
		return append([]byte(nil), p.BranchPageElement(0).Key()...)
	}
	if p.IsLeafPage() && p.Count() > 0 {
		return append([]byte(nil), p.LeafPageElement(0).Key()...)
	}
	return nil
}

func sortSpansDesc(spans []compactLiveSpan) {
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].start > spans[j].start
	})
}

// compactTailInfo computes the old high water mark, the new high water mark
// (one page above the highest live page plus the freelist reservation handled
// by the caller) and the number of pages in the free tail.
func (db *DB) compactTailInfo() (oldHWM, newHWM common.Pgid, freeTail common.Pgid, err error) {
	var live []compactLiveSpan
	if live, oldHWM, err = db.enumerateLiveSpans(); err != nil {
		return 0, 0, 0, err
	}
	if len(live) == 0 {
		// Empty DB: only the two meta pages exist logically.
		return oldHWM, 2, oldHWM - 2, nil
	}
	top := live[0] // enumerateLiveSpans sorts descending.
	highestLive := top.start + common.Pgid(top.size-1)
	newHWM = highestLive + 1
	if newHWM > oldHWM {
		newHWM = oldHWM
	}
	return oldHWM, newHWM, oldHWM - newHWM, nil
}

// compactShrink performs the final transaction that lowers the high water
// mark and physically truncates the file tail.
//
// The target high water mark is recomputed inside the write transaction (while
// holding the single writer lock) from the authoritative live-page layout, so
// a concurrent writer that landed between relocation and shrink cannot make
// the new high water mark inconsistent. The shrink is skipped (returns false,
// nil) if no free tail exists anymore or the freelist cannot be serialized
// below the new high water mark.
//
// Crash-safety ordering (all while holding the single writer lock):
//  1. drop free/pending pages >= newHWM from the freelist and commit the new
//     meta page (with the lower pgid) followed by fdatasync;
//  2. only then truncate the file to newHWM*pageSize bytes.
//
// A crash between steps leaves a valid (slightly oversized) database whose
// trailing pages are free; a subsequent run reclaims them. The truncate is
// performed while holding the mmap lock exclusively, so no read transaction
// can still be using an address that is being unmapped/faulted (and the file
// is only made shorter, never remapped: live addresses remain valid).
func (db *DB) compactShrink() (bool, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return false, err
	}

	// Authoritative tail computation under the writer lock.
	spans := enumerateLiveSpansFromTx(tx)
	oldHWM := tx.meta.Pgid()
	var highestLive common.Pgid = 1
	if len(spans) > 0 {
		highestLive = spans[0].start + common.Pgid(spans[0].size-1)
	}
	newHWM := highestLive + 1
	if newHWM > oldHWM {
		newHWM = oldHWM
	}
	if newHWM >= oldHWM {
		// A concurrent writer consumed the tail; nothing to shrink.
		tx.rollback()
		return false, nil
	}

	// Free the old freelist page (Commit does this normally), then allocate
	// the new freelist page strictly below the new high water mark. The shrink
	// transaction must never allocate at or beyond newHWM; if the required
	// contiguous space is not available below it (it can be blocked by pages
	// still pending for long readers), the shrink is aborted and the caller
	// retries later. Relocation commits remain durable.
	if tx.meta.Freelist() != common.PgidNoFreelist {
		tx.db.freelist.Free(tx.meta.Txid(), tx.db.page(tx.meta.Freelist()))
	}
	if !tx.db.NoFreelistSync {
		needed := (tx.db.freelist.EstimatedWritePageSize() / tx.db.pageSize) + 1
		tx.compactAllocLimit = newHWM
		p, allocErr := tx.allocate(needed)
		tx.compactAllocLimit = 0
		if allocErr != nil || p.Id()+common.Pgid(needed) > newHWM {
			tx.rollback()
			return false, ErrCompactionShrinkBlocked
		}
		tx.db.freelist.Write(p)
		tx.meta.SetFreelist(p.Id())
	} else {
		tx.meta.SetFreelist(common.PgidNoFreelist)
	}

	// Discard every free/pending page at or beyond the new high water mark.
	tx.db.freelist.DropAbove(newHWM)
	tx.meta.SetPgid(newHWM)

	// Write the freelist/data pages (only the freelist page is dirty) and
	// the meta page, with sync barriers, without running the generic grow
	// logic (the file is about to get shorter).
	if err := tx.write(); err != nil {
		tx.rollback()
		return false, err
	}
	if err := tx.writeMeta(); err != nil {
		// A meta write/fsync failure means the in-memory freelist can no
		// longer be trusted (the same convention as Tx.Commit). Ensure the
		// writer lock is released so the caller can close and reopen the DB;
		// the on-disk state is still the last fully committed transaction.
		tx.nonPhysicalRollback()
		return false, err
	}

	// Meta is now durable on disk. Closing the transaction releases the
	// read mmap guard (and keeps the single writer lock held; this is an RW
	// transaction). The physical truncate is performed afterwards so it can
	// acquire the mmap lock exclusively without a self-deadlock.
	shrinkSize := int64(newHWM) * int64(tx.db.pageSize)
	tx.close()

	// Truncate the file tail under exclusive mmap protection. Readers only
	// access pages below newHWM (they began after waitForReadersBefore, and
	// the writer lock blocks new writers).
	if err := db.physicalShrink(shrinkSize); err != nil {
		// Logical commit already happened and is durable; the oversized tail
		// will be reclaimed by a later run.
		return false, nil
	}

	// gofail: var afterOnlineCompactShrinkTruncate struct{}
	return true, nil
}

// physicalShrink truncates the data file to size bytes while holding the mmap
// lock exclusively. The existing mapping is intentionally kept because the
// file is only shortened and live pages remain at unchanged offsets.
func (db *DB) physicalShrink(size int64) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	// gofail: var beforeOnlineCompactShrinkTruncate string
	// return errors.New(beforeOnlineCompactShrinkTruncate)
	if err := db.file.Truncate(size); err != nil {
		return fmt.Errorf("online compact: file shrink error: %w", err)
	}
	// Flush the new file size metadata.
	// gofail: var beforeOnlineCompactShrinkSync struct{}
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("online compact: file shrink sync error: %w", err)
	}
	// gofail: var afterOnlineCompactShrinkSync struct{}
	return nil
}

// cursorStackContainsPage reports whether the cursor's traversal stack
// references the given page ID (either as a still-unmaterialized mmap page or
// as a materialized node).
func cursorStackContainsPage(c *Cursor, pgid common.Pgid) bool {
	for i := range c.stack {
		ref := &c.stack[i]
		if ref.node != nil && ref.node.pgid == pgid {
			return true
		}
		if ref.page != nil && ref.page.Id() == pgid {
			return true
		}
	}
	return false
}
