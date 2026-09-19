package freelist

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"go.etcd.io/bbolt/internal/common"
)

// pidSet holds the set of starting pgids which have the same span size
type pidSet map[common.Pgid]struct{}

type hashMap struct {
	*shared

	freePagesCount uint64                 // count of free pages(hashmap version)
	freemaps       map[uint64]pidSet      // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size
	forwardMap     map[common.Pgid]uint64 // key is start pgid, value is its span size
	backwardMap    map[common.Pgid]uint64 // key is end pgid, value is its span size
}

func (f *hashMap) Init(pgids common.Pgids) {
	// reset the counter when freelist init
	f.freePagesCount = 0
	f.freemaps = make(map[uint64]pidSet)
	f.forwardMap = make(map[common.Pgid]uint64)
	f.backwardMap = make(map[common.Pgid]uint64)

	if len(pgids) == 0 {
		return
	}

	if !sort.SliceIsSorted([]common.Pgid(pgids), func(i, j int) bool { return pgids[i] < pgids[j] }) {
		panic("pgids not sorted")
	}

	size := uint64(1)
	start := pgids[0]

	for i := 1; i < len(pgids); i++ {
		// continuous page
		if pgids[i] == pgids[i-1]+1 {
			size++
		} else {
			f.addSpan(start, size)

			size = 1
			start = pgids[i]
		}
	}

	// init the tail
	if size != 0 && start != 0 {
		f.addSpan(start, size)
	}

	f.reindex()
}

func (f *hashMap) Allocate(txid common.Txid, n int) common.Pgid {
	return f.AllocateInRange(txid, n, common.Pgid(math.MaxUint64))
}

func (f *hashMap) AllocateInRange(txid common.Txid, n int, maxStart common.Pgid) common.Pgid {
	if n == 0 {
		return 0
	}

	// Deterministic allocation: among all free spans that are large enough and
	// whose starting page id does not exceed maxStart, use the one with the
	// smallest starting page id.
	var pid common.Pgid
	var size uint64
	var found bool
	for start, spanSize := range f.forwardMap {
		if spanSize < uint64(n) || start > maxStart {
			continue
		}
		if !found || start < pid {
			pid = start
			size = spanSize
			found = true
		}
	}
	if !found {
		return 0
	}

	f.delSpan(pid, size)

	f.allocs[pid] = txid

	if remain := size - uint64(n); remain > 0 {
		f.addSpan(pid+common.Pgid(n), remain)
	}

	for i := common.Pgid(0); i < common.Pgid(n); i++ {
		delete(f.cache, pid+i)
	}
	return pid
}

// RemoveFreeIDs removes the given page ids from the free spans. The ids must
// be sorted and all of them must currently be free. Removing a sub-range of a
// span keeps the remaining prefix and suffix as separate free spans.
func (f *hashMap) RemoveFreeIDs(ids common.Pgids) {
	if len(ids) == 0 {
		return
	}
	sort.Sort(ids)

	for i := 0; i < len(ids); i++ {
		id := ids[i]
		if i > 0 && id <= ids[i-1] {
			panic(fmt.Sprintf("RemoveFreeIDs: ids must be strictly increasing: %d after %d", id, ids[i-1]))
		}

		spanStart, spanSize, ok := f.findContainingSpan(id)
		if !ok {
			panic(fmt.Sprintf("RemoveFreeIDs: page %d is not free", id))
		}

		// Find the end of the contiguous removed run within this span.
		runEnd := id
		for runEnd < spanStart+common.Pgid(spanSize)-1 {
			if i+1 >= len(ids) || ids[i+1] != runEnd+1 {
				break
			}
			i++
			runEnd++
		}

		f.delSpan(spanStart, spanSize)
		prefixLen := uint64(id - spanStart)
		suffixStart := runEnd + 1
		suffixLen := uint64(spanStart + common.Pgid(spanSize) - 1 - runEnd)
		if prefixLen > 0 {
			f.addSpan(spanStart, prefixLen)
		}
		if suffixLen > 0 {
			f.addSpan(suffixStart, suffixLen)
		}
	}
}

// findContainingSpan returns the size and starting page id of the free span
// containing the given page id.
func (f *hashMap) findContainingSpan(id common.Pgid) (start common.Pgid, size uint64, ok bool) {
	for s, spanSize := range f.forwardMap {
		if id >= s && id < s+common.Pgid(spanSize) {
			return s, spanSize, true
		}
	}
	return 0, 0, false
}

func (f *hashMap) TakeFreeSpan(txid common.Txid, start common.Pgid, n int) bool {
	spanStart, spanSize, ok := f.findContainingSpan(start)
	if !ok {
		return false
	}
	end := start + common.Pgid(n)
	if end > spanStart+common.Pgid(spanSize) {
		return false
	}

	f.delSpan(spanStart, spanSize)
	prefixLen := uint64(start - spanStart)
	suffixStart := start + common.Pgid(n)
	suffixLen := uint64(spanStart + common.Pgid(spanSize) - suffixStart)
	if prefixLen > 0 {
		f.addSpan(spanStart, prefixLen)
	}
	if suffixLen > 0 {
		f.addSpan(suffixStart, suffixLen)
	}
	f.allocs[start] = txid
	for i := common.Pgid(0); i < common.Pgid(n); i++ {
		delete(f.cache, start+i)
	}
	return true
}

func (f *hashMap) FreeCount() int {
	common.Verify(func() {
		expectedFreePageCount := f.hashmapFreeCountSlow()
		if int(f.freePagesCount) != expectedFreePageCount {
			panic(fmt.Sprintf("assertion failed: freePagesCount (%d) is out of sync with free pages map (%d)",
				f.freePagesCount, expectedFreePageCount))
		}
	})
	return int(f.freePagesCount)
}

func (f *hashMap) freePageIds() common.Pgids {
	count := f.FreeCount()
	if count == 0 {
		return common.Pgids{}
	}

	m := make([]common.Pgid, 0, count)

	startPageIds := make([]common.Pgid, 0, len(f.forwardMap))
	for k := range f.forwardMap {
		startPageIds = append(startPageIds, k)
	}
	sort.Sort(common.Pgids(startPageIds))

	for _, start := range startPageIds {
		if size, ok := f.forwardMap[start]; ok {
			for i := 0; i < int(size); i++ {
				m = append(m, start+common.Pgid(i))
			}
		}
	}

	return m
}

func (f *hashMap) hashmapFreeCountSlow() int {
	count := 0
	for _, size := range f.forwardMap {
		count += int(size)
	}
	return count
}

func (f *hashMap) addSpan(start common.Pgid, size uint64) {
	f.backwardMap[start-1+common.Pgid(size)] = size
	f.forwardMap[start] = size
	if _, ok := f.freemaps[size]; !ok {
		f.freemaps[size] = make(map[common.Pgid]struct{})
	}

	f.freemaps[size][start] = struct{}{}
	f.freePagesCount += size
}

func (f *hashMap) delSpan(start common.Pgid, size uint64) {
	delete(f.forwardMap, start)
	delete(f.backwardMap, start+common.Pgid(size-1))
	delete(f.freemaps[size], start)
	if len(f.freemaps[size]) == 0 {
		delete(f.freemaps, size)
	}
	f.freePagesCount -= size
}

func (f *hashMap) mergeSpans(ids common.Pgids) {
	if len(ids) == 0 {
		return
	}
	sort.Sort(ids)

	common.Verify(func() {
		ids1Freemap := f.idsFromFreemaps()
		ids2Forward := f.idsFromForwardMap()
		ids3Backward := f.idsFromBackwardMap()

		if !reflect.DeepEqual(ids1Freemap, ids2Forward) {
			panic(fmt.Sprintf("Detected mismatch, f.freemaps: %v, f.forwardMap: %v", f.freemaps, f.forwardMap))
		}
		if !reflect.DeepEqual(ids1Freemap, ids3Backward) {
			panic(fmt.Sprintf("Detected mismatch, f.freemaps: %v, f.backwardMap: %v", f.freemaps, f.backwardMap))
		}

		prev := common.Pgid(0)
		for _, id := range ids {
			// The ids shouldn't have duplicated free ID.
			if prev == id {
				panic(fmt.Sprintf("detected duplicated free ID: %d in ids: %v", id, ids))
			}
			prev = id

			// The ids shouldn't have any overlap with the existing f.freemaps.
			if _, ok := ids1Freemap[id]; ok {
				panic(fmt.Sprintf("detected overlapped free page ID: %d between ids: %v and existing f.freemaps: %v", id, ids, f.freemaps))
			}
		}
	})

	start := ids[0]
	end := ids[0]
	for i := 1; i < len(ids); i++ {
		id := ids[i]
		if id == end+1 {
			end = id
			continue
		}

		f.mergeWithExistingSpan(start, end)
		start, end = id, id
	}
	f.mergeWithExistingSpan(start, end)
}

// mergeWithExistingSpan merges free span [start, end] with adjacent existing free spans (both backward and forward).
func (f *hashMap) mergeWithExistingSpan(start, end common.Pgid) {
	prev := start - 1
	next := end + 1

	preSize, mergeWithPrev := f.backwardMap[prev]
	nextSize, mergeWithNext := f.forwardMap[next]
	newStart := start
	newSize := uint64(end - start + 1)

	if mergeWithPrev {
		// merge with previous span
		prevStart := prev + 1 - common.Pgid(preSize)
		f.delSpan(prevStart, preSize)

		newStart -= common.Pgid(preSize)
		newSize += preSize
	}

	if mergeWithNext {
		// merge with next span
		f.delSpan(next, nextSize)
		newSize += nextSize
	}

	f.addSpan(newStart, newSize)
}

// idsFromFreemaps get all free page IDs from f.freemaps.
// used by test only.
func (f *hashMap) idsFromFreemaps() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	for size, idSet := range f.freemaps {
		for start := range idSet {
			for i := 0; i < int(size); i++ {
				id := start + common.Pgid(i)
				if _, ok := ids[id]; ok {
					panic(fmt.Sprintf("detected duplicated free page ID: %d in f.freemaps: %v", id, f.freemaps))
				}
				ids[id] = struct{}{}
			}
		}
	}
	return ids
}

// idsFromForwardMap get all free page IDs from f.forwardMap.
// used by test only.
func (f *hashMap) idsFromForwardMap() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	for start, size := range f.forwardMap {
		for i := 0; i < int(size); i++ {
			id := start + common.Pgid(i)
			if _, ok := ids[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in f.forwardMap: %v", id, f.forwardMap))
			}
			ids[id] = struct{}{}
		}
	}
	return ids
}

// idsFromBackwardMap get all free page IDs from f.backwardMap.
// used by test only.
func (f *hashMap) idsFromBackwardMap() map[common.Pgid]struct{} {
	ids := make(map[common.Pgid]struct{})
	for end, size := range f.backwardMap {
		for i := 0; i < int(size); i++ {
			id := end - common.Pgid(i)
			if _, ok := ids[id]; ok {
				panic(fmt.Sprintf("detected duplicated free page ID: %d in f.backwardMap: %v", id, f.backwardMap))
			}
			ids[id] = struct{}{}
		}
	}
	return ids
}

func NewHashMapFreelist() Interface {
	hm := &hashMap{
		shared:      newShared(),
		freemaps:    make(map[uint64]pidSet),
		forwardMap:  make(map[common.Pgid]uint64),
		backwardMap: make(map[common.Pgid]uint64),
	}
	hm.Interface = hm
	return hm
}
