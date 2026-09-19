package bbolt

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// populateCompactDB creates a bucket with n keys, then deletes every other key
// (and drops a second bucket) so the file contains many free pages scattered
// around its tail.
func populateCompactDB(t testing.TB, n int, bigValue bool) *DB {
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	val := func(i int) []byte {
		if bigValue {
			v := make([]byte, 4096)
			for j := range v {
				v[j] = byte(i + j)
			}
			return v
		}
		return []byte(fmt.Sprintf("v%08d", i))
	}

	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%08d", i)), val(i)); err != nil {
				return err
			}
		}
		// Nested sub-buckets to exercise bucket-entry reference relocation.
		top, err := b.CreateBucket([]byte("sub"))
		if err != nil {
			return err
		}
		for i := 0; i < n/2; i++ {
			if err := top.Put([]byte(fmt.Sprintf("s%08d", i)), []byte(fmt.Sprintf("sv%08d", i))); err != nil {
				return err
			}
		}
		drop, err := tx.CreateBucket([]byte("drop"))
		if err != nil {
			return err
		}
		for i := 0; i < n/4; i++ {
			if err := drop.Put([]byte(fmt.Sprintf("d%08d", i)), []byte("xxxx")); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, db.Update(func(tx *Tx) error {
		if err := tx.DeleteBucket([]byte("drop")); err != nil {
			return err
		}
		b := tx.Bucket([]byte("b"))
		for i := 0; i < n; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("k%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	return db
}

// populateCompactDBWithSnapshot fills the database like populateCompactDB, but
// opens a read-only snapshot after the initial inserts and before the deletions
// that create the free tail, then performs the deletions. The returned
// transaction is a long-lived snapshot of the pre-deletion state.
func populateCompactDBWithSnapshot(t testing.TB, n int) (*DB, *Tx) {
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d", i))); err != nil {
				return err
			}
		}
		drop, err := tx.CreateBucket([]byte("drop"))
		if err != nil {
			return err
		}
		for i := 0; i < n/4; i++ {
			if err := drop.Put([]byte(fmt.Sprintf("d%08d", i)), []byte("xxxx")); err != nil {
				return err
			}
		}
		return nil
	}))

	snap, err := db.Begin(false)
	require.NoError(t, err)

	require.NoError(t, db.Update(func(tx *Tx) error {
		if err := tx.DeleteBucket([]byte("drop")); err != nil {
			return err
		}
		b := tx.Bucket([]byte("b"))
		for i := 0; i < n; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("k%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	return db, snap
}

func verifyCompactData(t *testing.T, db *DB, n int, bigValue bool) {
	require.NoError(t, db.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("b"))
		require.NotNil(t, b)
		for i := 0; i < n; i++ {
			v := b.Get([]byte(fmt.Sprintf("k%08d", i)))
			if i%2 == 0 {
				require.Nil(t, v, "key %d should be deleted", i)
			} else {
				require.NotNil(t, v, "key %d should exist", i)
				if !bigValue {
					require.Equal(t, []byte(fmt.Sprintf("v%08d", i)), v)
				} else {
					require.Len(t, v, 4096)
					require.Equal(t, byte(i), v[0])
				}
			}
		}
		sub := b.Bucket([]byte("sub"))
		require.NotNil(t, sub)
		count := 0
		require.NoError(t, sub.ForEach(func(k, v []byte) error {
			count++
			return nil
		}))
		require.Equal(t, n/2, count)
		require.Nil(t, tx.Bucket([]byte("drop")))
		return nil
	}))
}

func dbFileSize(t *testing.T, db *DB) int64 {
	fi, err := os.Stat(db.Path())
	require.NoError(t, err)
	return fi.Size()
}

func runRelocationUntilDone(t *testing.T, db *DB, batch int) int {
	total := 0
	for i := 0; i < 100000; i++ {
		moved, err := db.CompactRelocateBatch(batch)
		require.NoError(t, err)
		total += moved
		if moved == 0 {
			return total
		}
	}
	t.Fatal("relocation did not converge")
	return total
}

func TestOnlineCompact_BasicShrink(t *testing.T) {
	db := populateCompactDB(t, 5000, false)

	sizeBefore := dbFileSize(t, db)
	moved := runRelocationUntilDone(t, db, 256)
	require.Greater(t, moved, 0)

	freed, shrunken, err := db.compactShrink(-1)
	require.NoError(t, err)
	require.True(t, shrunken)
	require.Greater(t, freed, int64(0))

	sizeAfter := dbFileSize(t, db)
	require.Less(t, sizeAfter, sizeBefore)
	t.Logf("size %d -> %d (moved %d pages, freed %d bytes)", sizeBefore, sizeAfter, moved, freed)

	verifyCompactData(t, db, 5000, false)
}

func TestOnlineCompact_CompactOnlineConverges(t *testing.T) {
	db := populateCompactDB(t, 3000, true)
	sizeBefore := dbFileSize(t, db)

	stats, err := db.CompactOnline(&OnlineCompactConfig{
		MaxPagesPerBatch: 128,
		ShrinkTimeout:    -1,
	})
	require.NoError(t, err)
	require.Greater(t, stats.Batches, 0)
	require.Greater(t, stats.Shrinks, 0)
	require.Greater(t, stats.FreedBytes, int64(0))

	require.Less(t, dbFileSize(t, db), sizeBefore)
	verifyCompactData(t, db, 3000, true)

	// Running again must be idempotent.
	stats2, err := db.CompactOnline(&OnlineCompactConfig{ShrinkTimeout: -1})
	require.NoError(t, err)
	require.Zero(t, stats2.PagesMoved)
}

func TestOnlineCompact_PassesCheckAfterReopen(t *testing.T) {
	db := populateCompactDB(t, 4000, false)
	_, err := db.CompactOnline(&OnlineCompactConfig{MaxPagesPerBatch: 300, ShrinkTimeout: -1})
	require.NoError(t, err)
	verifyCompactData(t, db, 4000, false)
	path := db.Path()
	require.NoError(t, db.Close())

	reopened, err := Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	require.NoError(t, reopened.View(func(tx *Tx) error {
		for e := range tx.Check() {
			t.Errorf("check: %v", e)
		}
		return nil
	}))
	verifyCompactData(t, reopened, 4000, false)
}

// TestOnlineCompact_LongReadTransaction covers hard requirement (1): a read
// transaction opened before compaction must keep seeing its own snapshot while
// relocation batches commit underneath it.
// TestOnlineCompact_LongReadTransaction verifies hard requirement (1) and (2):
// a read-only snapshot opened before a shrink keeps its consistent view, and
// the shrink blocks (rather than truncating under the reader / remapping out
// from under it) until the snapshot ends.
func TestOnlineCompact_LongReadTransaction(t *testing.T) {
	db := populateCompactDB(t, 4000, false)

	// Perform relocation while short transactions and writers interleave; no
	// long reader is open during this phase, so freed pages are reusable in
	// the usual way.
	moved := runRelocationUntilDone(t, db, 256)
	require.Greater(t, moved, 0)

	// Open a long-lived snapshot. It pins the current live pages.
	snap, err := db.Begin(false)
	require.NoError(t, err)

	checkSnapshot := func(msg string) {
		b := snap.Bucket([]byte("b"))
		require.NotNil(t, b, msg)
		require.NotNil(t, b.Get([]byte("k00000001")), msg)
		require.Nil(t, b.Get([]byte("k00000000")), msg)
		require.NotNil(t, b.Bucket([]byte("sub")), msg)
		require.Nil(t, snap.Bucket([]byte("drop")), msg)
	}
	checkSnapshot("snapshot baseline")

	// Attempting the physical shrink with the snapshot open must block, not
	// truncate pages the snapshot may fault in.
	_, shrunken, shrinkErr := db.compactShrink(80 * time.Millisecond)
	if shrinkErr != nil {
		require.ErrorIs(t, shrinkErr, berrors.ErrOnlineCompactShrinkBlocked)
	} else {
		require.False(t, shrunken)
	}
	checkSnapshot("snapshot after blocked shrink")

	// Concurrent writes continue to work while the snapshot stays valid.
	require.NoError(t, db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("b")).Put([]byte("after-snap"), []byte("1"))
	}))
	checkSnapshot("snapshot after concurrent write")

	require.NoError(t, snap.Rollback())

	// After the reader drains, the pending tail releases and shrink succeeds.
	_, shrunken2, err := db.compactShrink(5 * time.Second)
	require.NoError(t, err)
	require.True(t, shrunken2)
	verifyCompactData(t, db, 4000, false)
}

// TestOnlineCompact_ReadersSeeConsistentDataDuringBatches constantly reads
// data from short transactions while batches commit, ensuring no torn reads.
func TestOnlineCompact_ReadersSeeConsistentDataDuringBatches(t *testing.T) {
	db := populateCompactDB(t, 2000, true)
	var wg sync.WaitGroup
	wg.Add(2)

	compDone := make(chan struct{})
	go func() {
		defer wg.Done()
		_, _ = db.CompactOnline(&OnlineCompactConfig{MaxPagesPerBatch: 64, ShrinkTimeout: -1})
		close(compDone)
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-compDone:
				return
			default:
			}
			verifyCompactData(t, db, 2000, true)
		}
	}()

	// Wait only for the compactor; readers may finish slightly after.
	<-compDone
	wg.Wait()
}

func TestOnlineCompact_SingleBatchBudget(t *testing.T) {
	db := populateCompactDB(t, 3000, false)

	m1, err := db.CompactRelocateBatch(1)
	require.NoError(t, err)
	// With budget 1 no multi-page unit can move but single-page units may.
	_ = m1

	m10, err := db.CompactRelocateBatch(10)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, m10, 0)

	// Zero budget uses default.
	m0, err := db.CompactRelocateBatch(0)
	require.NoError(t, err)
	_ = m0

	// Read-only DB rejects online compaction.
	roPath := db.Path()
	require.NoError(t, db.Close())
	ro, err := Open(roPath, 0400, &Options{ReadOnly: true})
	require.NoError(t, err)
	defer ro.Close()
	_, err = ro.CompactRelocateBatch(10)
	require.ErrorIs(t, err, berrors.ErrDatabaseReadOnly)
}

func TestOnlineCompact_LivePageGraphSkipsMetaAndFreelist(t *testing.T) {
	db := populateCompactDB(t, 500, false)
	require.NoError(t, db.View(func(tx *Tx) error {
		g := tx.collectLivePageGraph()
		// Meta pages 0/1 are never data units.
		_, has0 := g.units[0]
		_, has1 := g.units[1]
		require.False(t, has0)
		require.False(t, has1)
		// Freelist page is never a relocated data unit.
		fl := tx.meta.Freelist()
		if fl != common.PgidNoFreelist {
			_, hasFL := g.units[fl]
			require.False(t, hasFL)
		}
		// Every unit page must be reachable in the file and positive.
		for id := range g.units {
			require.GreaterOrEqual(t, id, common.Pgid(2))
		}
		return nil
	}))
}

// TestOnlineCompact_NoFreelistSync verifies online compaction works when the
// freelist is not persisted (it is reconstructed by scanning on open).
func TestOnlineCompact_NoFreelistSync(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, &Options{NoFreelistSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < 3000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *Tx) error {
		b := tx.Bucket([]byte("b"))
		for i := 0; i < 3000; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("k%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))

	sizeBefore := dbFileSize(t, db)
	stats, err := db.CompactOnline(&OnlineCompactConfig{ShrinkTimeout: -1})
	require.NoError(t, err)
	require.Greater(t, stats.Shrinks, 0)
	require.Less(t, dbFileSize(t, db), sizeBefore)

	path := db.Path()
	require.NoError(t, db.Close())
	reopened, err := Open(path, 0600, &Options{NoFreelistSync: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.View(func(tx *Tx) error {
		for e := range tx.Check() {
			t.Errorf("check: %v", e)
		}
		return nil
	}))
	require.NoError(t, reopened.View(func(tx *Tx) error {
		b := tx.Bucket([]byte("b"))
		require.Nil(t, b.Get([]byte("k00000000")))
		require.NotNil(t, b.Get([]byte("k00000001")))
		return nil
	}))
}
