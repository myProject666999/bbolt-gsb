package freelist

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

func freelistBackends() map[string]Interface {
	return map[string]Interface{
		"array":   NewArrayFreelist(),
		"hashmap": NewHashMapFreelist(),
	}
}

// TestFreelist_AllocateInRange verifies the bounded allocation used by online
// compaction: it must (a) pick the lowest eligible span, (b) honor maxStart,
// and (c) keep overflow suffixes below a strict end limit.
func TestFreelist_AllocateInRange(t *testing.T) {
	for name, f := range freelistBackends() {
		t.Run(name, func(t *testing.T) {
			f.Init(common.Pgids{3, 4, 5, 6, 9, 10, 20, 21, 22})

			// Lowest fit of size 2 is [3,4].
			require.Equal(t, common.Pgid(3), f.AllocateInRange(100, 2, 20))
			// Next lowest fit of size 2 is [5,6].
			require.Equal(t, common.Pgid(5), f.AllocateInRange(101, 2, 19))
			// Span [9,10] is the lowest fit with maxStart 9.
			require.Equal(t, common.Pgid(9), f.AllocateInRange(102, 2, 9))
			// maxStart 8 excludes both [9,10] (taken) and [20,21,22].
			require.Equal(t, common.Pgid(0), f.AllocateInRange(103, 2, 8))
			// [20,21,22] is reachable with a higher maxStart.
			require.Equal(t, common.Pgid(20), f.AllocateInRange(104, 2, 20))
			require.Equal(t, common.Pgids{22}, f.FreePageIDs())
		})
	}
}

func TestFreelist_TakeFreeSpan(t *testing.T) {
	for name, f := range freelistBackends() {
		t.Run(name, func(t *testing.T) {
			f.Init(common.Pgids{3, 4, 5, 6, 10, 11})

			// Exact middle removal splits the span.
			require.True(t, f.TakeFreeSpan(100, 4, 1))
			require.Equal(t, common.Pgids{3, 5, 6, 10, 11}, f.FreePageIDs())
			require.False(t, f.Freed(4))

			// Multi-page take at the end.
			require.True(t, f.TakeFreeSpan(101, 10, 2))
			require.Equal(t, common.Pgids{3, 5, 6}, f.FreePageIDs())

			// [5,6] is a contiguous 2-page span.
			require.True(t, f.TakeFreeSpan(102, 5, 2))
			require.Equal(t, common.Pgids{3}, f.FreePageIDs())
			// A partially-present run fails.
			require.False(t, f.TakeFreeSpan(103, 3, 2))
		})
	}
}

func TestFreelist_TakeFreeSpanSuffix(t *testing.T) {
	for name, f := range freelistBackends() {
		t.Run(name, func(t *testing.T) {
			f.Init(common.Pgids{5, 6, 10})
			require.True(t, f.TakeFreeSpan(1, 5, 2))
			require.Equal(t, common.Pgids{10}, f.FreePageIDs())
			require.False(t, f.TakeFreeSpan(2, 5, 2))
			require.False(t, f.TakeFreeSpan(3, 9, 1))
		})
	}
}

func TestFreelist_RemoveFreeIDs(t *testing.T) {
	for name, f := range freelistBackends() {
		t.Run(name, func(t *testing.T) {
			f.Init(common.Pgids{3, 4, 5, 6, 10, 11, 12})

			// Remove a prefix of the first span and a whole span.
			f.RemoveFreeIDs(common.Pgids{3, 10, 11, 12})
			require.Equal(t, common.Pgids{4, 5, 6}, f.FreePageIDs())

			// Remove a middle element, leaving a prefix and suffix.
			f.RemoveFreeIDs(common.Pgids{5})
			require.Equal(t, common.Pgids{4, 6}, f.FreePageIDs())

			// Removing a non-free page must panic (allocation invariants).
			require.Panics(t, func() { f.RemoveFreeIDs(common.Pgids{4, 7}) })
		})
	}
}

func TestFreelist_PendingPageIDs(t *testing.T) {
	// Page 10 is live (not in the freelist); page 11 is free.
	f := NewArrayFreelist()
	f.Init(common.Pgids{11, 12})
	require.Empty(t, f.PendingPageIDs())

	// Free the live page under txid 5; it is pending until readers release it.
	p := common.NewPage(10, common.LeafPageFlag, 0, 0)
	f.Free(5, p)
	require.ElementsMatch(t, common.Pgids{10}, f.PendingPageIDs())
	require.NotContains(t, f.FreePageIDs(), common.Pgid(10))

	// With no readonly transactions, beginRWTx-style release makes it free.
	f.ReleasePendingPages()
	require.Contains(t, f.FreePageIDs(), common.Pgid(10))
	require.Empty(t, f.PendingPageIDs())
}
