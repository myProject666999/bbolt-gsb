package freelist

import (
	"go.etcd.io/bbolt/internal/common"
)

type ReadWriter interface {
	// Read calls Init with the page ids stored in the given page.
	Read(page *common.Page)

	// Write writes the freelist into the given page.
	Write(page *common.Page)

	// EstimatedWritePageSize returns the size in bytes of the freelist after serialization in Write.
	// This should never underestimate the size.
	EstimatedWritePageSize() int
}

type Interface interface {
	ReadWriter

	// Init initializes this freelist with the given list of pages.
	Init(ids common.Pgids)

	// Allocate tries to allocate the given number of contiguous pages
	// from the free list pages. It returns the starting page ID if
	// available; otherwise, it returns 0.
	Allocate(txid common.Txid, numPages int) common.Pgid

	// AllocateBelow behaves like Allocate but only returns a contiguous
	// block of numPages free pages whose highest used page ID is strictly
	// smaller than below. It is used by online incremental compaction to
	// guarantee that a migrated page is placed at a lower page ID than the
	// page it replaces. It returns 0 if no such block exists.
	AllocateBelow(txid common.Txid, numPages int, below common.Pgid) common.Pgid

	// Count returns the number of free and pending pages.
	Count() int

	// FreeCount returns the number of free pages.
	FreeCount() int

	// PendingCount returns the number of pending pages.
	PendingCount() int

	// AddReadonlyTXID adds a given read-only transaction id for pending page tracking.
	AddReadonlyTXID(txid common.Txid)

	// RemoveReadonlyTXID removes a given read-only transaction id for pending page tracking.
	RemoveReadonlyTXID(txid common.Txid)

	// ReleasePendingPages releases any pages associated with closed read-only transactions.
	ReleasePendingPages()

	// Free releases a page and its overflow for a given transaction id.
	// If the page is already free or is one of the meta pages, then a panic will occur.
	Free(txId common.Txid, p *common.Page)

	// Freed returns whether a given page is in the free list.
	Freed(pgId common.Pgid) bool

	// Rollback removes the pages from a given pending tx.
	Rollback(txId common.Txid)

	// DropAbove permanently removes from the freelist (both the available
	// free pages and the pending pages) every page with an ID >= pgid. It is
	// used when online incremental compaction lowers the database high
	// water mark: the pages beyond the new high water mark are discarded
	// together with the trailing file space instead of being reused.
	DropAbove(pgid common.Pgid)

	// ReadonlyTxIDs returns a sorted copy of the IDs of all currently open
	// read-only transactions known to the freelist.
	ReadonlyTxIDs() []common.Txid

	// Copyall copies a list of all free ids and all pending ids in one sorted list.
	// f.count returns the minimum length required for dst.
	Copyall(dst []common.Pgid)

	// Reload reads the freelist from a page and filters out pending items.
	Reload(p *common.Page)

	// NoSyncReload reads the freelist from Pgids and filters out pending items.
	NoSyncReload(pgIds common.Pgids)

	// freePageIds returns the IDs of all free pages. Returns an empty slice if no free pages are available.
	freePageIds() common.Pgids

	// pendingPageIds returns all pending pages by transaction id.
	pendingPageIds() map[common.Txid]*txPending

	// release moves all page ids for a transaction id (or older) to the freelist.
	release(txId common.Txid)

	// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
	releaseRange(begin, end common.Txid)

	// mergeSpans is merging the given pages into the freelist
	mergeSpans(ids common.Pgids)
}
