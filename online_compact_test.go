package bbolt

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// fragmentedDB builds a database with many pages at high addresses, frees a
// substantial fraction of the keys (creating a fragmented freelist), and
// reopens it so the freed pages are released from the pending list. It returns
// the opened DB wrapper together with the bucket name and total key count.
func fragmentedDB(t *testing.T, opts *OnlineCompactOptions) (*DB, []byte, int) {
	_ = opts
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, nil)
	require.NoError(t, err)

	bucketName := []byte("data")
	const numTx = 20
	const keysPerTx = 200
	err = db.Update(func(tx *Tx) error {
		_, e := tx.CreateBucket(bucketName)
		return e
	})
	require.NoError(t, err)

	for tr := 0; tr < numTx; tr++ {
		err = db.Update(func(tx *Tx) error {
			b := tx.Bucket(bucketName)
			for i := 0; i < keysPerTx; i++ {
				k := compactKey(tr, i)
				v := bytes.Repeat([]byte{byte(i)}, 180)
				if e := b.Put(k, v); e != nil {
					return e
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Delete every second key to create many free pages interleaved with
	// live ones.
	err = db.Update(func(tx *Tx) error {
		b := tx.Bucket(bucketName)
		for tr := 0; tr < numTx; tr++ {
			for i := 0; i < keysPerTx; i += 2 {
				if e := b.Delete(compactKey(tr, i)); e != nil {
					return e
				}
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, db.Close())
	db, err = Open(f, 0600, nil)
	require.NoError(t, err)
	return db, bucketName, numTx * keysPerTx
}

func compactKey(tr, i int) []byte {
	return []byte(fmt.Sprintf("%05d-%05d", tr, i))
}

func verifyCompactData(t *testing.T, db *DB, bucketName []byte, numTx, keysPerTx, deletedStride int) {
	t.Helper()
	err := db.View(func(tx *Tx) error {
		b := tx.Bucket(bucketName)
		require.NotNil(t, b)
		for tr := 0; tr < numTx; tr++ {
			for i := 0; i < keysPerTx; i++ {
				v := b.Get(compactKey(tr, i))
				if deletedStride > 0 && i%deletedStride == 0 {
					require.Nil(t, v, "key %d/%d should be deleted", tr, i)
					continue
				}
				require.NotNil(t, v, "missing key %d/%d", tr, i)
				require.Equal(t, 180, len(v))
				require.Equal(t, byte(i), v[0])
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func checkDB(t *testing.T, db *DB) {
	t.Helper()
	err := db.View(func(tx *Tx) error {
		for err := range tx.Check() {
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

func fileSize(t *testing.T, db *DB) int64 {
	fi, err := os.Stat(db.Path())
	require.NoError(t, err)
	return fi.Size()
}

func dbHWM(t *testing.T, db *DB) common.Pgid {
	var hwm common.Pgid
	err := db.View(func(tx *Tx) error {
		hwm = tx.meta.Pgid()
		return nil
	})
	require.NoError(t, err)
	return hwm
}

// TestOnlineCompact_Basic shrinks a fragmented database while preserving all
// data and passing bbolt's consistency check before and after.
func TestOnlineCompact_Basic(t *testing.T) {
	db, bucketName, total := fragmentedDB(t, nil)
	defer db.Close()

	before := fileSize(t, db)
	hwmBefore := dbHWM(t, db)
	checkDB(t, db)

	stats, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, stats.Shrunk, "expected physical shrink, stats=%+v", stats)
	require.Greater(t, stats.MovedPages, 0)
	require.Equal(t, hwmBefore, stats.HighWaterBefore)
	require.Less(t, stats.HighWaterAfter, hwmBefore)

	after := fileSize(t, db)
	require.Less(t, after, before)
	require.Equal(t, int64(stats.HighWaterAfter)*int64(db.pageSize), after)

	verifyCompactData(t, db, bucketName, 20, 200, 2)
	checkDB(t, db)
	_ = total
}

// TestOnlineCompact_Idempotent shows a second run is a no-op.
func TestOnlineCompact_Idempotent(t *testing.T) {
	db, bucketName, _ := fragmentedDB(t, nil)
	defer db.Close()

	stats1, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, stats1.Shrunk)
	sz1 := fileSize(t, db)

	stats2, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.False(t, stats2.Shrunk)
	require.Equal(t, sz1, fileSize(t, db))

	verifyCompactData(t, db, bucketName, 20, 200, 2)
}

// TestOnlineCompact_ConcurrentLongReader is the key visibility test: a
// read-only transaction opened before compaction must keep reading its own
// snapshot for its whole lifetime while compaction relocates pages and shrinks
// the file.
func TestOnlineCompact_ConcurrentLongReader(t *testing.T) {
	db, bucketName, _ := fragmentedDB(t, nil)
	defer db.Close()

	// Long-lived reader snapshot taken before compaction.
	reader, err := db.Begin(false)
	require.NoError(t, err)
	defer reader.Rollback()

	// The snapshot includes every key present when it was taken: keys with
	// even i had already been deleted before reopening, so only odd remain.
	snapshotOK := make(chan struct{})
	go func() {
		// First read while compaction has not started.
		b := reader.Bucket(bucketName)
		v := b.Get(compactKey(0, 1))
		require.NotNil(t, v)
		close(snapshotOK)
	}()
	<-snapshotOK

	// Run compaction while the reader stays open. The shrink must be deferred
	// (no SIGBUS / no dangling mapping) because the reader still holds the
	// pre-shrink snapshot.
	stats, err := db.OnlineCompact(&OnlineCompactOptions{ShrinkTimeout: -1})
	require.ErrorIs(t, err, ErrCompactionShrinkBlocked)
	require.Greater(t, stats.MovedPages, 0)
	require.True(t, stats.ShrinkDeferred, "shrink must be deferred while reader is open")
	require.False(t, stats.Shrunk)
	sizeDuringReader := fileSize(t, db)

	// The old snapshot remains readable: traverse the whole snapshot and touch
	// many pages that were relocated underneath it.
	var count int
	c := reader.Bucket(bucketName).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		require.NotNil(t, v)
		count++
	}
	require.Equal(t, 20*100, count, "snapshot must keep all 2000 surviving keys")
	require.NoError(t, reader.Rollback())

	// Once the reader closes, rerun compaction: physical shrink happens.
	stats2, err := db.OnlineCompact(&OnlineCompactOptions{ShrinkTimeout: 5e9})
	require.NoError(t, err)
	require.True(t, stats2.Shrunk)
	require.Less(t, fileSize(t, db), sizeDuringReader)

	verifyCompactData(t, db, bucketName, 20, 200, 2)
	checkDB(t, db)
}

// TestOnlineCompact_ConcurrentWriters keeps a background workload writing
// throughout the whole run, proving compaction is online (no stop-the-world).
func TestOnlineCompact_ConcurrentWriters(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	wdb, werr := Open(f, 0600, nil)
	require.NoError(t, werr)
	bucketName := []byte("data")
	require.NoError(t, wdb.Update(func(tx *Tx) error {
		b, e := tx.CreateBucket(bucketName)
		if e != nil {
			return e
		}
		for i := 0; i < 4000; i++ {
			if e := b.Put(compactKey(i/200, i%200), make([]byte, 120)); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, wdb.Update(func(tx *Tx) error {
		b := tx.Bucket(bucketName)
		for i := 0; i < 4000; i += 2 {
			if e := b.Delete(compactKey(i/200, i%200)); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, wdb.Close())
	wdb, werr = Open(f, 0600, nil)
	require.NoError(t, werr)
	db := wdb
	defer db.Close()

	stop := make(chan struct{})
	var wroteN int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			// A modest, throttled workload: compaction must coexist with
			// writes, but the test does not need write saturation.
			time.Sleep(2 * time.Millisecond)
			err := db.Update(func(tx *Tx) error {
				b := tx.Bucket(bucketName)
				k := compactKey(i%20, 1+2*(i%100))
				return b.Put(k, bytes.Repeat([]byte{byte(1)}, 180))
			})
			if err != nil {
				return
			}
			atomic.AddInt64(&wroteN, 1)
			i++
		}
	}()

	var stats OnlineCompactStats
	var compactErr error
	for attempt := 0; attempt < 200; attempt++ {
		stats, compactErr = db.OnlineCompact(&OnlineCompactOptions{BatchPages: 16, ShrinkTimeout: 30e9})
		if compactErr == nil {
			break
		}
		// Online compaction is optimistic: the live-page snapshot used by a
		// relocation transaction may race with the background writers, in
		// which case the run aborts and is retried. A shrink can also be
		// briefly blocked by internal read transactions.
		var retryable bool
		if errorIsAny(compactErr, ErrCompactionShrinkBlocked, ErrCompactionRetryable) {
			retryable = true
		}
		require.True(t, retryable, "unexpected compaction error: %v", compactErr)
	}
	require.NoError(t, compactErr)
	close(stop)
	wg.Wait()
	require.True(t, stats.Shrunk)
	require.Greater(t, atomic.LoadInt64(&wroteN), int64(0))
	checkDB(t, db)

	// Data count remains intact.
	err := db.View(func(tx *Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		n := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			n++
		}
		require.Equal(t, 2000, n)
		return nil
	})
	require.NoError(t, err)
}

// TestOnlineCompact_Reopen reopens the database after compaction and verifies
// check + data through a fresh process-level view (freelist reloaded from
// disk).
func TestOnlineCompact_Reopen(t *testing.T) {
	db, bucketName, _ := fragmentedDB(t, nil)
	path := db.Path()
	stats, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, stats.Shrunk)
	require.NoError(t, db.Close())

	db2, err := Open(path, 0600, nil)
	require.NoError(t, err)
	defer db2.Close()

	verifyCompactData(t, db2, bucketName, 20, 200, 2)
	checkDB(t, db2)
}

func errorIsAny(err error, targets ...error) bool {
	for _, tg := range targets {
		if errors.Is(err, tg) {
			return true
		}
	}
	return false
}

// TestOnlineCompact_EmptyDB makes sure an empty/new database compacts without
// shrinking below the meta pages and without errors.
func TestOnlineCompact_EmptyDB(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, nil)
	require.NoError(t, err)
	defer db.Close()
	before := fileSize(t, db)
	stats, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.False(t, stats.Shrunk)
	require.Equal(t, before, fileSize(t, db))
	checkDB(t, db)
}

// TestOnlineCompact_NoFreelistSync covers the freelist-not-persisted path.
func TestOnlineCompact_NoFreelistSync(t *testing.T) {
	db0, _, _ := fragmentedDB(t, nil)
	path := db0.Path()
	require.NoError(t, db0.Close())

	db, err := Open(path, 0600, &Options{NoFreelistSync: true})
	require.NoError(t, err)
	defer db.Close()
	before := fileSize(t, db)
	stats, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, stats.Shrunk, "stats=%+v", stats)
	require.Less(t, fileSize(t, db), before)
	checkDB(t, db)
}

// TestOnlineCompact_NestedBucketsAndOverflow uses nested buckets and a large
// value that occupies multiple (overflow) pages.
func TestOnlineCompact_NestedBucketsAndOverflow(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	db, err := Open(f, 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Update(func(tx *Tx) error {
		top, e := tx.CreateBucket([]byte("top"))
		if e != nil {
			return e
		}
		mid, e := top.CreateBucket([]byte("mid"))
		if e != nil {
			return e
		}
		leaf, e := mid.CreateBucket([]byte("leaf"))
		if e != nil {
			return e
		}
		for i := 0; i < 500; i++ {
			if e := leaf.Put(compactKey(i/100, i%100), bytes.Repeat([]byte{byte(i)}, 200)); e != nil {
				return e
			}
		}
		// A large value spanning multiple pages.
		return top.Put([]byte("big"), bytes.Repeat([]byte("Z"), 3*db.pageSize))
	}))
	// Add churn then delete half to fragment the file.
	require.NoError(t, db.Update(func(tx *Tx) error {
		leaf := tx.Bucket([]byte("top")).Bucket([]byte("mid")).Bucket([]byte("leaf"))
		for i := 0; i < 500; i += 2 {
			if e := leaf.Delete(compactKey(i/100, i%100)); e != nil {
				return e
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
	db, err = Open(f, 0600, nil)
	require.NoError(t, err)

	before := fileSize(t, db)
	stats, err := db.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, stats.Shrunk, "stats=%+v", stats)
	require.Less(t, fileSize(t, db), before)

	// Validate nested bucket data and the big overflow value.
	require.NoError(t, db.View(func(tx *Tx) error {
		leaf := tx.Bucket([]byte("top")).Bucket([]byte("mid")).Bucket([]byte("leaf"))
		require.NotNil(t, leaf)
		n := 0
		c := leaf.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			require.Len(t, v, 200)
			n++
		}
		require.Equal(t, 250, n)
		big := tx.Bucket([]byte("top")).Get([]byte("big"))
		require.Len(t, big, 3*db.pageSize)
		return nil
	}))
	checkDB(t, db)
}
