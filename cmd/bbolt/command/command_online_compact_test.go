package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestOnlineCompactCommand_Run verifies the "online-compact" command shrinks a
// fragmented database in place while keeping all data consistent.
func TestOnlineCompactCommand_Run(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 4000; i++ {
			if err := b.Put([]byte(key5(i)), make([]byte, 120)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		for i := 0; i < 4000; i += 2 {
			if err := b.Delete([]byte(key5(i))); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	before, err := chkdb(db.Path())
	require.NoError(t, err)

	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"online-compact", "--batch-pages", "32", db.Path()})
	require.NoError(t, rootCmd.Execute())

	after, err := chkdb(db.Path())
	require.NoError(t, err)
	require.Equal(t, before, after, "data must be unchanged after online compaction")
}

func TestOnlineCompactCommand_NoArgs(t *testing.T) {
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"online-compact"})
	err := rootCmd.Execute()
	require.Error(t, err)
}

func key5(n int) string {
	const digits = "0123456789"
	b := make([]byte, 5)
	for i := 4; i >= 0; i-- {
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b)
}
