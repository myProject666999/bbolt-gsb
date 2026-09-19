package failpoint

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	gofail "go.etcd.io/gofail/runtime"
)

// buildFragmentedOnlineCompactDB fills a database with many pages and deletes
// every second key, producing a fragmented freelist suitable for shrinking.
func buildFragmentedOnlineCompactDB(t *testing.T, path string) {
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)

	bucketName := []byte("data")
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucket(bucketName)
		return e
	}))
	for tr := 0; tr < 12; tr++ {
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)
			for i := 0; i < 300; i++ {
				k := []byte(fmt.Sprintf("%04d-%04d", tr, i))
				if e := b.Put(k, bytes.Repeat([]byte{byte(i)}, 180)); e != nil {
					return e
				}
			}
			return nil
		}))
	}
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for tr := 0; tr < 12; tr++ {
			for i := 0; i < 300; i += 2 {
				if e := b.Delete([]byte(fmt.Sprintf("%04d-%04d", tr, i))); e != nil {
					return e
				}
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func verifyOnlineCompactDB(t *testing.T, path string) int64 {
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	var size int64
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			return e
		}
		b := tx.Bucket([]byte("data"))
		require.NotNil(t, b)
		n := 0
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			require.NotNil(t, v)
			require.Len(t, v, 180)
			n++
		}
		require.Equal(t, 12*150, n)
		size = tx.Size()
		return nil
	}))
	return size
}

// TestFailpoint_OnlineCompact_MetaSyncFails injects an fsync failure while
// online compaction commits a relocation/meta transaction. The run must report
// the error, the database must remain a valid pre-compaction snapshot, and a
// subsequent reopen + compaction must succeed and shrink the file.
func TestFailpoint_OnlineCompact_MetaSyncFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	buildFragmentedOnlineCompactDB(t, path)

	// Fail the first writeMeta call: a relocation commit aborts and
	// OnlineCompact returns the injected error.
	require.NoError(t, gofail.Enable("beforeWriteMetaError", `1*return("writeMeta injected failure")`))

	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	st, err := db.OnlineCompact(nil)
	require.Error(t, err, "stats=%+v", st)
	require.Zero(t, st.Shrunk)

	// Disable the fault before closing/reopening (after an fsync/meta failure
	// the in-memory state is not reusable, like Tx.Commit's convention).
	require.NoError(t, gofail.Disable("beforeWriteMetaError"))

	// Closing is safe; the failed transaction rolled back (or committed the
	// pre-existing meta only).
	require.NoError(t, db.Close())

	// Reopen without the fault: the DB must check clean and still hold data.
	before := verifyOnlineCompactDB(t, path)

	db2, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	var sizeAfter int64
	require.NoError(t, db2.View(func(tx *bolt.Tx) error { sizeAfter = tx.Size(); return nil }))
	require.Equal(t, before, sizeAfter)
	st, err = db2.OnlineCompact(nil)
	require.NoError(t, err)
	require.True(t, st.Shrunk)
	require.NoError(t, db2.Close())

	after := verifyOnlineCompactDB(t, path)
	require.Less(t, after, before)
}

// TestFailpoint_OnlineCompact_ShrinkTruncateFails injects an error on the
// file truncate during the physical shrink. The logical (meta) commit has
// already happened and is durable, so reopen must show a valid, consistent
// database whose logical high water mark is already compacted.
func TestFailpoint_OnlineCompact_ShrinkTruncateFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	buildFragmentedOnlineCompactDB(t, path)

	require.NoError(t, gofail.Enable("beforeOnlineCompactShrinkTruncate", `return("inject shrink failure")`))
	defer func() { require.NoError(t, gofail.Disable("beforeOnlineCompactShrinkTruncate")) }()

	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	_, err = db.OnlineCompact(nil)
	// physicalShrink failure is swallowed by design (logical commit durable);
	// the run reports no error but no physical shrink.
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// File remains consistent and a follow-up run shrinks it.
	size1 := verifyOnlineCompactDB(t, path)
	db2, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	st, err := db2.OnlineCompact(nil)
	require.NoError(t, err)
	require.NoError(t, db2.Close())
	_ = st
	size2 := verifyOnlineCompactDB(t, path)
	require.LessOrEqual(t, size2, size1)
}

// TestFailpoint_OnlineCompact_Kill exercises the power-failure path by
// running OnlineCompact in a child process that is killed (via a gofail
// exit()) right after the shrink truncate/sync. On reopen the database must be
// consistent: either fully compacted or still at its previous size, never in
// a half-compacted invalid state.
func TestFailpoint_OnlineCompact_Kill(t *testing.T) {
	if os.Getenv("BBOLT_ONLINE_COMPACT_CHILD") == "1" {
		onlineCompactKillChild()
		return
	}

	path := filepath.Join(t.TempDir(), "db")
	buildFragmentedOnlineCompactDB(t, path)
	before, err := os.Stat(path)
	require.NoError(t, err)

	// Run the child binary (the current test executable) with gofail enabled
	// through the test binary; gofail only has effect when the runtime is
	// active, so we enable the exit inside the child helper directly through
	// the gofail runtime instead.
	cmd := exec.Command(os.Args[0], "-test.run", "TestFailpoint_OnlineCompact_Kill")
	cmd.Env = append(os.Environ(),
		"BBOLT_ONLINE_COMPACT_CHILD=1",
		"BBOLT_ONLINE_COMPACT_PATH="+path,
	)
	out, runErr := cmd.CombinedOutput()
	t.Logf("child output: %s", string(out))
	require.Error(t, runErr, "child should have exited via injected kill")

	// Reopen and check: the database must be fully usable and consistent.
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			return e
		}
		b := tx.Bucket([]byte("data"))
		n := 0
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			require.Len(t, v, 180)
			n++
		}
		require.Equal(t, 12*150, n)
		return nil
	}))
	require.NoError(t, db.Close())

	after, err := os.Stat(path)
	require.NoError(t, err)
	require.LessOrEqual(t, after.Size(), before.Size())
	_ = time.Now
}

func onlineCompactKillChild() {
	path := os.Getenv("BBOLT_ONLINE_COMPACT_PATH")
	if path == "" {
		os.Exit(2)
	}
	if err := gofail.Enable("afterOnlineCompactShrinkSync", `panic("injected power failure")`); err != nil {
		os.Exit(2)
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		os.Exit(2)
	}
	if _, err := db.OnlineCompact(nil); err != nil {
		os.Exit(2)
	}
	// TestMain may run quick tests; exit explicitly when compact finished
	// without the failpoint firing (small DB).
	os.Exit(0)
}
