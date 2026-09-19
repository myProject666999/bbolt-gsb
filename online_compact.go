package bbolt

import (
	"sort"
	"time"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// DefaultOnlineCompactBatchPages is the default maximum number of live pages
// relocated by a single online compaction batch.
const DefaultOnlineCompactBatchPages = 256

// OnlineCompactConfig configures an online incremental compaction run.
//
// Online compaction relocates live pages in small batches, each committed as
// a regular transaction while the database keeps serving other read and write
// transactions. Once enough data has moved out of the file tail, and no
// read-only transaction still needs the tail pages, the data file is shrunk.
type OnlineCompactConfig struct {
	// MaxPagesPerBatch limits how many live pages a single relocation
	// transaction may move. If <= 0, DefaultOnlineCompactBatchPages is used.
	MaxPagesPerBatch int

	// MaxBatches bounds how many relocation batches CompactOnline performs.
	// If <= 0 there is no bound and compaction runs until no more progress
	// can be made.
	MaxBatches int

	// ShrinkTimeout is the maximum time to wait for open read-only
	// transactions (which pin the old mmap) before a file shrink. A zero
	// value uses a 30 second default; a negative value disables waiting.
	ShrinkTimeout time.Duration

	// DisableShrink skips truncating the data file. Relocation still runs,
	// so fragmentation is reduced and subsequent normal allocations reuse
	// the reclaimed free pages, but disk space is not returned to the OS.
	DisableShrink bool
}

// OnlineCompactStats reports the work performed by one CompactOnline run.
type OnlineCompactStats struct {
	// Batches is the number of relocation transactions committed.
	Batches int
	// PagesMoved is the number of live pages copied to new locations.
	PagesMoved int
	// Shrinks is the number of times the data file was truncated.
	Shrinks int
	// FreedBytes is the number of bytes returned to the OS by shrinks.
	FreedBytes int64
}

// CompactOnline performs incremental, online compaction of the database.
//
// Unlike Compact, which rewrites the whole database into a separate file and
// requires exclusive access to the source, CompactOnline operates on an open,
// fully serving DB: each batch is one ordinary read-write transaction, so
// concurrent View transactions keep seeing their own consistent snapshots and
// concurrent writers are merely serialized with the batch in the usual way.
//
// The on-disk format is unchanged; old bbolt versions can open files produced
// (and partially compacted) by this function. If read-only transactions keep
// tail pages alive, relocation still progresses and the shrink is retried
// later instead of forcing the readers off their snapshot.
//
// A nil config uses default settings.
func (db *DB) CompactOnline(cfg *OnlineCompactConfig) (*OnlineCompactStats, error) {
	if cfg == nil {
		cfg = &OnlineCompactConfig{}
	}
	maxPages := cfg.MaxPagesPerBatch
	if maxPages <= 0 {
		maxPages = DefaultOnlineCompactBatchPages
	}

	stats := &OnlineCompactStats{}
	for batch := 0; cfg.MaxBatches <= 0 || batch < cfg.MaxBatches; batch++ {
		moved, err := db.CompactRelocateBatch(maxPages)
		if err != nil {
			return stats, err
		}
		if moved > 0 {
			stats.Batches++
			stats.PagesMoved += moved
		}

		if !cfg.DisableShrink {
			freed, shrunken, err := db.compactShrink(cfg.ShrinkTimeout)
			if err != nil {
				return stats, err
			}
			if shrunken {
				stats.Shrinks++
				stats.FreedBytes += freed
			} else if moved == 0 {
				// Nothing left to relocate and the tail cannot be reclaimed
				// right now (pinned by readers or no free suffix). Progress
				// resumes the next time CompactOnline is called.
				return stats, nil
			}
		} else if moved == 0 {
			return stats, nil
		}
	}
	return stats, nil
}

// livePageGraph describes the pages reachable from the live metadata snapshot.
type livePageGraph struct {
	// units maps the starting page id of every live span (a single page or
	// an overflow run) to its length in pages.
	units map[common.Pgid]int
	// parents maps a live page id to the id of the page that references it.
	// A page referenced from an inline bucket value maps to the leaf page
	// hosting that value. The b-tree has a single parent chain per page.
	parents map[common.Pgid]common.Pgid
	// bucketRoots lists the root page id of every (sub-)bucket, including the
	// top level root.
	bucketRoots []common.Pgid
}

// collectLivePageGraph builds the live page graph for the current meta snapshot
// of the transaction.
func (tx *Tx) collectLivePageGraph() *livePageGraph {
	graph := &livePageGraph{
		units:   make(map[common.Pgid]int),
		parents: make(map[common.Pgid]common.Pgid),
	}

	var visitBucketRoot func(rootID, parent common.Pgid)
	var visitPage func(id common.Pgid, parent common.Pgid)
	visitPage = func(id common.Pgid, parent common.Pgid) {
		if id < 2 {
			// Inline pages use pgid 0/1 as a fake id and are not on disk.
			return
		}
		p := tx.page(id)
		size := int(p.Overflow()) + 1
		if _, seen := graph.units[id]; seen {
			return
		}
		graph.units[id] = size
		if parent >= 2 {
			graph.parents[id] = parent
		}

		if p.IsBranchPage() {
			for i := range p.BranchPageElements() {
				visitPage(p.BranchPageElement(uint16(i)).Pgid(), id)
			}
		} else if p.IsLeafPage() {
			for i := range p.LeafPageElements() {
				elem := p.LeafPageElement(uint16(i))
				if elem.IsBucketEntry() {
					if bkt := elem.Bucket(); bkt.RootPage() >= 2 {
						visitBucketRoot(bkt.RootPage(), id)
					}
				}
			}
		}
	}
	visitBucketRoot = func(rootID, parent common.Pgid) {
		graph.bucketRoots = append(graph.bucketRoots, rootID)
		visitPage(rootID, parent)
	}

	visitBucketRoot(tx.meta.RootBucket().RootPage(), 0)

	// Top-level buckets referenced from the root bucket are discovered while
	// visiting the root pages above (leaf bucket entries), so nothing else is
	// needed here.
	return graph
}

// freeSpan is a contiguous run of currently available free pages.
type freeSpan struct {
	start common.Pgid
	size  int
}

// relocationPlanner mirrors the deterministic lowest-fit allocation used by
// both freelist backends (see hashMap.AllocateInRange / array.AllocateInRange).
// It lets a relocation transaction decide where pages will land before any
// freelist state is mutated, so a unit can be skipped entirely when its whole
// parent chain cannot be placed.
type relocationPlanner struct {
	spans []freeSpan // kept sorted by start
}

func newRelocationPlanner(freeIDs common.Pgids) *relocationPlanner {
	sort.Sort(freeIDs)
	p := &relocationPlanner{}
	for i := 0; i < len(freeIDs); {
		start := freeIDs[i]
		j := i + 1
		for j < len(freeIDs) && freeIDs[j] == freeIDs[j-1]+1 {
			j++
		}
		p.spans = append(p.spans, freeSpan{start: start, size: j - i})
		i = j
	}
	return p
}

// allocate reserves n contiguous pages starting at a page id not exceeding
// maxStart, preferring the lowest eligible start. It returns 0 when no such
// span exists.
func (p *relocationPlanner) allocate(n int, maxStart common.Pgid) common.Pgid {
	idx := -1
	for i := range p.spans {
		if p.spans[i].size >= n && p.spans[i].start <= maxStart {
			idx = i
			break
		}
	}
	if idx < 0 {
		return 0
	}
	s := p.spans[idx]
	dest := s.start
	s.start += common.Pgid(n)
	s.size -= n
	if s.size == 0 {
		p.spans = append(p.spans[:idx], p.spans[idx+1:]...)
	} else {
		p.spans[idx] = s
	}
	return dest
}

// allocateEndingBefore reserves n contiguous pages whose final page id is
// strictly below limit, preferring the lowest eligible start. Unlike allocate
// with maxStart, this also constrains the end of multi-page (overflow) spans.
func (p *relocationPlanner) allocateEndingBefore(n int, limit common.Pgid) common.Pgid {
	return p.allocate(n, limit-common.Pgid(n))
}

// freeIDs returns the page ids still available after the reservations made so
// far, in ascending order.
func (p *relocationPlanner) freeIDs() common.Pgids {
	var ids common.Pgids
	for _, s := range p.spans {
		for i := 0; i < s.size; i++ {
			ids = append(ids, s.start+common.Pgid(i))
		}
	}
	return ids
}

func (p *relocationPlanner) hasSpanAt(start common.Pgid, n int) bool {
	for _, s := range p.spans {
		if s.start == start {
			return s.size >= n
		}
		if s.start > start {
			return false
		}
	}
	return false
}

func plannerHasSpanAt(p *relocationPlanner, start common.Pgid, n int) bool {
	return p.hasSpanAt(start, n)
}

func (p *relocationPlanner) takeSpanAt(start common.Pgid, n int) {
	idx := sort.Search(len(p.spans), func(i int) bool { return p.spans[i].start >= start })
	if idx >= len(p.spans) || p.spans[idx].start != start || p.spans[idx].size < n {
		panic("relocation planner: takeSpanAt on missing span")
	}
	s := p.spans[idx]
	s.start += common.Pgid(n)
	s.size -= n
	if s.size == 0 {
		p.spans = append(p.spans[:idx], p.spans[idx+1:]...)
	} else {
		p.spans[idx] = s
	}
}

// CompactRelocateBatch performs a single online compaction batch: it copies at
// most maxPages tail live pages into free holes closer to the beginning of the
// file and atomically updates all referencing parent pages.
//
// It returns the number of live pages relocated (0 when there is no eligible
// work) and an error if the relocation transaction could not be committed.
// The database keeps serving transactions concurrently; pages are only freed
// through the regular freelist snapshot machinery.
func (db *DB) CompactRelocateBatch(maxPages int) (int, error) {
	if db.readOnly {
		return 0, berrors.ErrDatabaseReadOnly
	}
	if maxPages <= 0 {
		maxPages = DefaultOnlineCompactBatchPages
	}

	tx, err := db.Begin(true)
	if err != nil {
		return 0, err
	}

	graph := tx.collectLivePageGraph()
	planner := newRelocationPlanner(db.freelist.FreePageIDs())

	// Live units in descending page-id order so the highest tail pages move
	// first.
	units := make([]common.Pgid, 0, len(graph.units))
	for id := range graph.units {
		units = append(units, id)
	}
	sort.Sort(sort.Reverse(common.Pgids(units)))

	var movedPages int

	remap := make(map[common.Pgid]common.Pgid)

	for _, src := range units {
		size := graph.units[src]

		// The unit itself must land strictly before its current position so
		// that the live high water mark can only decrease.
		dest := planner.allocate(size, src-1)
		// Skip this unit (but keep scanning smaller units) when there is no
		// lower free hole or the batch page budget is exhausted.
		if dest == 0 || movedPages+size > maxPages {
			if dest != 0 {
				planner.spans = insertAllocatedSpan(planner.spans, dest, size)
			}
			continue
		}

		// Tentatively record the unit and every page on its ancestor chain
		// (each rewritten to a location strictly before the unit's start).
		// The reservation order here is exactly the order in which
		// applyRelocation replays allocations against the real freelist, so
		// the planned destinations are guaranteed to match.
		type reserved struct {
			pageID common.Pgid
			dest   common.Pgid
		}
		tentative := []reserved{{pageID: src, dest: dest}}
		tentativeIDs := map[common.Pgid]struct{}{src: {}}
		tentativePages := size
		ok := true
		for _, ancestor := range chainList(graph, src) {
			if _, done := remap[ancestor]; done {
				continue
			}
			if _, done := tentativeIDs[ancestor]; done {
				continue
			}
			ancestorSize := graph.units[ancestor]
			if movedPages+tentativePages+ancestorSize > maxPages {
				ok = false
				break
			}
			ancestorDest := planner.allocate(ancestorSize, src-1)
			if ancestorDest == 0 {
				ok = false
				break
			}
			tentative = append(tentative, reserved{pageID: ancestor, dest: ancestorDest})
			tentativeIDs[ancestor] = struct{}{}
			tentativePages += ancestorSize
		}
		if !ok {
			// Roll back every tentative reservation of this unit.
			for _, r := range tentative {
				planner.spans = insertAllocatedSpan(planner.spans, r.dest, graph.units[r.pageID])
			}
			continue
		}
		for _, r := range tentative {
			remap[r.pageID] = r.dest
		}
		movedPages += tentativePages
	}

	if len(remap) == 0 {
		_ = tx.Rollback()
		return 0, nil
	}

	if err := tx.applyRelocation(remap); err != nil {
		tx.rollback()
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return movedPages, nil
}

// chainList returns the ancestor page chain of a live unit, from the unit's
// direct parent up to the root page.
func chainList(graph *livePageGraph, unit common.Pgid) []common.Pgid {
	var chain []common.Pgid
	cur, ok := graph.parents[unit]
	for ok {
		chain = append(chain, cur)
		cur, ok = graph.parents[cur]
	}
	return chain
}

// insertAllocatedSpan re-adds a span [start, start+size) into the planner,
// merging it with adjacent spans. It is used to roll back tentative
// reservations.
func insertAllocatedSpan(spans []freeSpan, start common.Pgid, size int) []freeSpan {
	idx := sort.Search(len(spans), func(i int) bool { return spans[i].start >= start })
	spans = append(spans, freeSpan{})
	copy(spans[idx+1:], spans[idx:])
	spans[idx] = freeSpan{start: start, size: size}

	// Merge with previous.
	if idx > 0 && spans[idx-1].start+common.Pgid(spans[idx-1].size) == start {
		spans[idx-1].size += size
		copy(spans[idx:], spans[idx+1:])
		spans = spans[:len(spans)-1]
		idx--
	}
	// Merge with next.
	if idx+1 < len(spans) && spans[idx].start+common.Pgid(spans[idx].size) == spans[idx+1].start {
		spans[idx].size += spans[idx+1].size
		copy(spans[idx+1:], spans[idx+2:])
		spans = spans[:len(spans)-1]
	}
	return spans
}

// applyRelocation executes a planned page relocation inside the write
// transaction. remap maps old page ids to new ones. For every remapped page a
// fresh dirty copy is allocated at the planned destination; parent pages that
// reference remapped children are copied as well so that open read-only
func (tx *Tx) applyRelocation(remap map[common.Pgid]common.Pgid) error {
	graph := tx.collectLivePageGraph()

	// Every remapped page already has a planned destination. Allocate the
	// real pages exactly there, consuming the same freelist spans the
	// relocation planner reserved.
	oldIDs := make(common.Pgids, 0, len(remap))
	for old := range remap {
		oldIDs = append(oldIDs, old)
	}
	sort.Sort(oldIDs)

	// Allocate in the exact reservation order: unit first, then its ancestor
	// chain, repeating per descending unit. Reconstruct that order by
	// replaying the planner is complex; instead allocate at a fixed address
	// (allocateAt) which removes any dependence on iteration order.
	for _, old := range oldIDs {
		size := graph.units[old]
		if err := tx.allocateAt(size, remap[old]); err != nil {
			return err
		}
	}

	// Copy each old page into its fresh destination and rewrite every
	// outgoing page reference that points at a relocated page.
	for _, old := range oldIDs {
		oldPage := tx.db.page(old)
		size := graph.units[old]
		newID := remap[old]
		newPage := tx.pages[newID]
		copyPageBytes(newPage, oldPage, tx.db.pageSize, size)
		newPage.SetId(newID)
		patchPageRefs(newPage, remap)
	}

	// Update the top level bucket root.
	if dest, ok := remap[tx.meta.RootBucket().RootPage()]; ok {
		tx.meta.RootBucket().SetRootPage(dest)
		// tx.root embeds its own copy of the top level bucket header; keep it
		// in sync because Tx.Commit publishes tx.root.RootPage() to the meta.
		tx.root.SetRootPage(dest)
	}

	// Free every old span through the regular snapshot machinery so that old
	// read-only transactions keep their pages until they finish.
	for _, old := range oldIDs {
		tx.db.freelist.Free(tx.meta.Txid(), tx.db.page(old))
	}

	return nil
}

// allocateAt reserves the contiguous span [id, id+n) from the current free
// list. The span must currently be free; it is removed from the freelist and
// registered as allocated by this transaction.
func (tx *Tx) allocateAt(n int, id common.Pgid) error {
	if !tx.db.freelist.TakeFreeSpan(tx.meta.Txid(), id, n) {
		return berrors.ErrOnlineCompactNoSpace
	}

	var buf []byte
	if n == 1 {
		buf = tx.db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, n*tx.db.pageSize)
	}
	p := (*common.Page)(unsafe.Pointer(&buf[0]))
	p.SetOverflow(uint32(n - 1))
	p.SetId(id)
	tx.pages[id] = p
	tx.stats.IncPageCount(int64(n))
	tx.stats.IncPageAlloc(int64(n * tx.db.pageSize))
	return nil
}

// copyPageBytes copies n pages starting at src into the buffer backing dst.
func copyPageBytes(dst, src *common.Page, pageSize int, n int) {
	size := uintptr(n) * uintptr(pageSize)
	srcBytes := common.UnsafeByteSlice(unsafe.Pointer(src), 0, 0, int(size))
	dstBytes := common.UnsafeByteSlice(unsafe.Pointer(dst), 0, 0, int(size))
	copy(dstBytes, srcBytes)
}

// patchPageRefs rewrites outgoing page references on a freshly copied page so
// that references to relocated pages point at their new ids.
func patchPageRefs(p *common.Page, remap map[common.Pgid]common.Pgid) {
	switch {
	case p.IsBranchPage():
		for i := range p.BranchPageElements() {
			elem := p.BranchPageElement(uint16(i))
			if dest, ok := remap[elem.Pgid()]; ok {
				elem.SetPgid(dest)
			}
		}
	case p.IsLeafPage():
		for i := range p.LeafPageElements() {
			elem := p.LeafPageElement(uint16(i))
			if !elem.IsBucketEntry() {
				continue
			}
			bkt := elem.Bucket()
			if bkt.RootPage() >= 2 {
				if dest, ok := remap[bkt.RootPage()]; ok {
					bkt.SetRootPage(dest)
				}
			}
		}
	}
}

const defaultShrinkTimeout = 30 * time.Second

// compactShrink attempts one physical shrink of the data file. It opens a
// write transaction (which first releases pending pages of closed readers),
// checks that the whole file tail is free and no longer pinned by any open
// read-only transaction, rewrites the freelist below the new high water mark
// and commits. Only after the new metadata is durable is the file truncated
// and the mapping adjusted, so a crash at any point leaves a database that
// reopens consistently.
//
// freed is the number of bytes removed; shrunken is false when the tail is
// not reclaimable yet.
func (db *DB) compactShrink(timeout time.Duration) (freed int64, shrunken bool, err error) {
	if db.readOnly {
		return 0, false, berrors.ErrDatabaseReadOnly
	}

	if timeout == 0 {
		timeout = defaultShrinkTimeout
	}

	tx, err := db.Begin(true)
	if err != nil {
		return 0, false, err
	}

	hwm := tx.meta.Pgid()
	// Physical file size in pages. grow() pre-allocates beyond the high water
	// mark; pages in [hwm, filePages) contain nothing the metadata references
	// and can always be returned.
	fileBytes, err := db.fileSize()
	if err != nil {
		_ = tx.Rollback()
		return 0, false, err
	}
	filePages := common.Pgid(fileBytes / db.pageSize)

	// The suffix must begin strictly after the highest live page, otherwise
	// the new high water mark would cut off reachable data (the top level
	// root included).
	graph := tx.collectLivePageGraph()
	var highestLive common.Pgid
	for start, size := range graph.units {
		if end := start + common.Pgid(size) - 1; end > highestLive {
			highestLive = end
		}
	}

	// Within [2, hwm) reclaim the longest contiguous suffix that is currently
	// available in the freelist. Pages in [hwm, filePages) are appended to
	// that suffix implicitly.
	suffixStart := firstFreeSuffix(hwm, db.freelist.FreePageIDs())
	if suffixStart <= highestLive {
		suffixStart = highestLive + 1
	}
	// Pending (not yet released) pages are invisible to FreePageIDs but may
	// still be faulted in by an open read-only transaction. Trim the suffix
	// upward past any pending page still inside it.
	for {
		var blocking common.Pgid
		for _, pid := range db.freelist.PendingPageIDs() {
			if pid >= suffixStart && pid < hwm && pid >= blocking {
				blocking = pid
			}
		}
		if blocking == 0 {
			break
		}
		suffixStart = blocking + 1
	}
	if suffixStart >= filePages {
		_ = tx.Rollback()
		return 0, false, nil
	}

	if db.NoFreelistSync {
		// No freelist is persisted; reopen reconstructs it by scanning. Just
		// lower the high-water mark to the free suffix and truncate. Drop any
		// in-memory free ids at/above the new high-water mark.
		db.freelist.RemoveFreeIDs(freeIDsAbove(db.freelist.FreePageIDs(), suffixStart))
		tx.meta.SetPgid(suffixStart)
		if err = tx.Commit(); err != nil {
			return 0, false, err
		}
		newSizeBytes := int64(suffixStart) * int64(db.pageSize)
		if err = db.shrinkFile(newSizeBytes, timeout); err != nil {
			return 0, false, err
		}
		return int64(filePages-suffixStart) * int64(db.pageSize), true, nil
	}

	// Place the new freelist at the very beginning of the reclaimed suffix
	// using currently available free pages, and set the new high water mark
	// immediately after it. The actual freelist allocation/write happens in
	// Tx.Commit (which first frees the old freelist); a per-transaction bound
	// keeps that allocation within the suffix.
	needed := (db.freelist.EstimatedWritePageSize() / db.pageSize) + 1
	available := db.freelist.FreePageIDs()
	planner := newRelocationPlanner(available)

	// Prefer placing the new freelist in a free hole strictly before the
	// reclaimed suffix, so it does not consume reclaimable tail space. If no
	// such hole fits, fall back to the first pages of the suffix itself.
	flStart := planner.allocateEndingBefore(needed, suffixStart)
	if flStart != 0 {
		// Take those pages out of the candidate set; the suffix computation
		// below must ignore them.
		available = planner.freeIDs()
	} else {
		// Otherwise place the freelist exactly at the start of the suffix.
		// Those pages are free by construction of the contiguous free suffix.
		if !plannerHasSpanAt(planner, suffixStart, needed) {
			_ = tx.Rollback()
			return 0, false, nil
		}
		planner.takeSpanAt(suffixStart, needed)
		available = planner.freeIDs()
		flStart = suffixStart
	}
	newHWM := flStart + common.Pgid(needed)
	// If the freelist sits inside the suffix, the hwm must follow it and the
	// remainder of the suffix is still truncated. If it sits below
	// suffixStart, keep the hwm at suffixStart.
	if newHWM < suffixStart {
		newHWM = suffixStart
	}
	if newHWM > filePages {
		_ = tx.Rollback()
		return 0, false, nil
	}
	freeSet := make(map[common.Pgid]struct{}, len(available))
	for _, id := range available {
		freeSet[id] = struct{}{}
	}
	// Validate the suffix pages beyond the freelist placement are free /
	// beyond the old hwm.
	validateFrom := suffixStart
	if flStart >= suffixStart {
		validateFrom = flStart + common.Pgid(needed)
	}
	for id := validateFrom; id < newHWM; id++ {
		if _, free := freeSet[id]; !free && id < hwm {
			_ = tx.Rollback()
			return 0, false, nil
		}
	}
	// Everything at or beyond the new high water mark is about to disappear
	// with the file tail. Drop those ids from the freelist so the serialized
	// freelist never references pages that will be beyond EOF after a crash.
	drop := make(common.Pgids, 0)
	for id := range freeSet {
		if id >= newHWM {
			drop = append(drop, id)
		}
	}
	db.freelist.RemoveFreeIDs(drop)

	tx.freelistAllocLimit = newHWM
	tx.freelistAllocStart = flStart
	tx.meta.SetPgid(newHWM)

	if err = tx.Commit(); err != nil {
		return 0, false, err
	}

	// gofail: var afterShrinkCommit struct{}

	newSizeBytes := int64(newHWM) * int64(db.pageSize)
	if err = db.shrinkFile(newSizeBytes, timeout); err != nil {
		// The metadata is already committed; the file is just larger than
		// needed. Report the shrink failure but the database is consistent.
		return 0, false, err
	}

	return int64(filePages-newHWM) * int64(db.pageSize), true, nil
}

// firstFreeSuffix returns the first page id of the longest contiguous free
// suffix below hwm, or hwm when there is no free tail.
func firstFreeSuffix(hwm common.Pgid, freeIDs common.Pgids) common.Pgid {
	want := hwm - 1
	for i := len(freeIDs) - 1; i >= 0; i-- {
		if freeIDs[i] == want {
			want--
			continue
		}
		if freeIDs[i] < want {
			break
		}
	}
	return want + 1
}

// freeIDsAbove returns the subset of ids that are at or above limit.
func freeIDsAbove(ids common.Pgids, limit common.Pgid) common.Pgids {
	out := make(common.Pgids, 0)
	for _, id := range ids {
		if id >= limit {
			out = append(out, id)
		}
	}
	return out
}
