package freelist

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

func testAllocateBelow(t *testing.T, f Interface) {
	f.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})

	// Lowest fitting single page below a boundary.
	require.Equal(t, common.Pgid(3), f.AllocateBelow(1, 1, 10))

	// Fresh freelist for contiguous tests.
	g := newImpl(f)
	g.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})
	// Lowest fitting contiguous block of two below 10 -> [4,5] (3 already used
	// logically in this example's fresh copy: [3,4] is the answer).
	require.Equal(t, common.Pgid(3), g.AllocateBelow(1, 2, 10))

	// A block whose highest page reaches the boundary is rejected. Below 6 a
	// three-page block could only be [3,4,5] (valid, highest id 5 < 6), so use
	// a boundary of 5: no three-page block fits strictly below page 5.
	h := newImpl(f)
	h.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})
	require.Equal(t, common.Pgid(0), h.AllocateBelow(1, 3, 5))
	require.Equal(t, common.Pgid(3), h.AllocateBelow(1, 3, 8)) // [3,4,5]

	// No free page below the first free id.
	j := newImpl(f)
	j.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})
	require.Equal(t, common.Pgid(0), j.AllocateBelow(1, 1, 3))

	// Both backends must pick the lowest fitting span when several fit:
	// below 19 single pages resolve in increasing order.
	k := newImpl(f)
	k.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})
	require.Equal(t, common.Pgid(3), k.AllocateBelow(1, 1, 19))
	require.Equal(t, common.Pgid(4), k.AllocateBelow(2, 1, 19))
	require.Equal(t, common.Pgid(5), k.AllocateBelow(3, 1, 19))

	// Large contiguous request splits a bigger span (hashmap) or walks it
	// (array); highest used id must stay below the boundary.
	l := newImpl(f)
	l.Init([]common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18})
	id := l.AllocateBelow(1, 4, 8)
	require.Equal(t, common.Pgid(3), id)
}

func TestFreelist_AllocateBelow_Array(t *testing.T) {
	testAllocateBelow(t, NewArrayFreelist())
}

func TestFreelist_AllocateBelow_HashMap(t *testing.T) {
	testAllocateBelow(t, NewHashMapFreelist())
}

func testDropAbove(t *testing.T, f Interface) {
	f.Init([]common.Pgid{3, 4, 5, 9, 12, 13, 18})
	f.DropAbove(12)
	require.Equal(t, common.Pgids{3, 4, 5, 9}, f.freePageIds())
	require.False(t, f.Freed(12))
	require.False(t, f.Freed(13))
	require.False(t, f.Freed(18))
	require.True(t, f.Freed(9))

	// Dropping within a contiguous hash span keeps the surviving prefix.
	f2 := f
	_ = f2
	g := newImpl(f)
	g.Init([]common.Pgid{20, 21, 22, 23, 40})
	g.DropAbove(22)
	require.Equal(t, common.Pgids{20, 21}, g.freePageIds())

	// Pending pages above the boundary are dropped too.
	h := newImpl(f)
	page := common.NewPage(50, common.LeafPageFlag, 0, 2)
	page.SetId(50)
	h.Free(100, page) // frees [50,51,52] pending
	require.Equal(t, 3, h.PendingCount())
	h.DropAbove(51)
	// pgid 50 is below the boundary and survives in the pending tx; 51 and 52
	// are discarded.
	require.True(t, h.Freed(50))
	require.False(t, h.Freed(51))
	require.False(t, h.Freed(52))
}

func newImpl(iface Interface) Interface {
	if _, ok := iface.(*array); ok {
		return NewArrayFreelist()
	}
	return NewHashMapFreelist()
}

func TestFreelist_DropAbove_Array(t *testing.T) {
	testDropAbove(t, NewArrayFreelist())
}

func TestFreelist_DropAbove_HashMap(t *testing.T) {
	testDropAbove(t, NewHashMapFreelist())
}

func TestFreelist_ReadonlyTxIDs(t *testing.T) {
	for _, f := range []Interface{NewArrayFreelist(), NewHashMapFreelist()} {
		f.AddReadonlyTXID(7)
		f.AddReadonlyTXID(2)
		f.AddReadonlyTXID(5)
		require.Equal(t, []common.Txid{2, 5, 7}, f.ReadonlyTxIDs())
		f.RemoveReadonlyTXID(5)
		require.Equal(t, []common.Txid{2, 7}, f.ReadonlyTxIDs())
	}
}
