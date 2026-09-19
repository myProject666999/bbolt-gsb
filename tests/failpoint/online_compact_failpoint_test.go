package failpoint

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	gofail "go.etcd.io/gofail/runtime"
)

func fillCompactionDB(t *testing.T, path string, n int) {
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%08d", i)), []byte(fmt.Sprintf("v%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		for i := 0; i < n; i += 2 {
			if err := b.Delete([]byte(fmt.Sprintf("k%08d", i))); err != nil {
				return err
			}
		}
		return nil
	}))
	for {
		moved, err := db.CompactRelocateBatch(256)
		require.NoError(t, err)
		if moved == 0 {
			break
		}
	}
	require.NoError(t, db.Close())
}

func reopenAndCheck(t *testing.T, path string, n int) {
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			t.Errorf("check after failure: %v", e)
		}
		return nil
	}))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		require.NotNil(t, b)
		for i := 0; i < n; i++ {
			v := b.Get([]byte(fmt.Sprintf("k%08d", i)))
			if i%2 == 0 {
				require.Nil(t, v)
			} else {
				require.Equal(t, []byte(fmt.Sprintf("v%08d", i)), v)
			}
		}
		return nil
	}))
}

func fpFileSize(t *testing.T, path string) int64 {
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

func TestFailpoint_OnlineCompact_ShrinkFileError(t *testing.T) {
	const n = 5000
	path := filepath.Join(t.TempDir(), "db")
	fillCompactionDB(t, path, n)

	require.NoError(t, gofail.Enable("shrinkFileError", `return("injected shrink failure")`))
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	_, err = db.CompactOnline(&bolt.OnlineCompactConfig{MaxPagesPerBatch: 256, ShrinkTimeout: -1})
	require.ErrorContains(t, err, "injected shrink failure")
	require.NoError(t, db.Close())
	require.NoError(t, gofail.Disable("shrinkFileError"))

	reopenAndCheck(t, path, n)

	before := fpFileSize(t, path)
	db2, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	stats, err := db2.CompactOnline(&bolt.OnlineCompactConfig{ShrinkTimeout: -1})
	require.NoError(t, err)
	require.NoError(t, db2.Close())
	require.Less(t, fpFileSize(t, path), before)
	require.Greater(t, stats.Shrinks, 0)
	reopenAndCheck(t, path, n)
}

func TestFailpoint_OnlineCompact_CrashAfterShrinkCommit(t *testing.T) {
	const n = 5000
	if os.Getenv("BBOLT_COMPACT_CRASH_CHILD") == "1" {
		crashAfterShrinkCommitChild(n)
		return
	}

	path := filepath.Join(t.TempDir(), "db")
	fillCompactionDB(t, path, n)
	before := fpFileSize(t, path)

	cmd := exec.Command(os.Args[0], "-test.run", "TestFailpoint_OnlineCompact_CrashAfterShrinkCommit")
	cmd.Env = append(os.Environ(),
		"BBOLT_COMPACT_CRASH_CHILD=1",
		"BBOLT_COMPACT_DB="+path,
	)
	out, _ := cmd.CombinedOutput()
	require.Contains(t, string(out), "BBOLT_COMPACT_CRASHED", "child output:\n%s", out)

	require.Equal(t, before, fpFileSize(t, path))
	reopenAndCheck(t, path, n)

	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	_, err = db.CompactOnline(&bolt.OnlineCompactConfig{ShrinkTimeout: -1})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Less(t, fpFileSize(t, path), before)
	reopenAndCheck(t, path, n)
}

func crashAfterShrinkCommitChild(n int) {
	if err := gofail.Enable("afterShrinkCommit", `panic("BBOLT_COMPACT_CRASHED")`); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	path := os.Getenv("BBOLT_COMPACT_DB")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, err := db.CompactOnline(&bolt.OnlineCompactConfig{ShrinkTimeout: -1}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_ = db.Close()
	os.Exit(0)
}

// TestFailpoint_OnlineCompact_DataSyncFail injects an fsync failure while a
// relocation batch writes its data pages. The batch rolls back; the reopened
// database must be consistent and keep the pre-batch data.
func TestFailpoint_OnlineCompact_DataSyncFail(t *testing.T) {
	const n = 4000
	path := filepath.Join(t.TempDir(), "db")
	fillCompactionDB(t, path, n)

	require.NoError(t, gofail.Enable("beforeSyncDataPages", `return("injected data fsync failure")`))
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		if _, err := db.CompactRelocateBatch(128); err != nil {
			require.ErrorContains(t, err, "injected data fsync failure")
			break
		}
	}
	require.NoError(t, db.Close())
	require.NoError(t, gofail.Disable("beforeSyncDataPages"))

	reopenAndCheck(t, path, n)

	// Compaction completes normally once the failure is cleared.
	db2, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	_, err = db2.CompactOnline(&bolt.OnlineCompactConfig{ShrinkTimeout: -1})
	require.NoError(t, err)
	require.NoError(t, db2.Close())
	reopenAndCheck(t, path, n)
}
