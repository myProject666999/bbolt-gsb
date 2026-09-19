package command_test

import (
	"bytes"
	crypto "crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestCompactOnlineCommand_Run(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("keep"))
		if err != nil {
			return err
		}
		for i := 0; i < 2000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i))); err != nil {
				return err
			}
		}
		drop, err := tx.CreateBucketIfNotExists([]byte("large_vals"))
		if err != nil {
			return err
		}
		for i := 0; i < 6; i++ {
			v := make([]byte, 500*1024)
			_, _ = crypto.Read(v)
			if err := drop.Put([]byte(fmt.Sprintf("l%d", i)), v); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte("large_vals"))
	}))

	path := db.Path()
	db.MustClose()

	expected, err := chkdb(path)
	require.NoError(t, err)

	before, err := os.Stat(path)
	require.NoError(t, err)

	var out bytes.Buffer
	rootCmd := command.NewRootCommand()
	rootCmd.SetOut(&out)
	rootCmd.SetArgs([]string{
		"compact-online",
		"--pages-per-batch", "256",
		"--shrink-timeout", "1s",
		path,
	})
	require.NoError(t, rootCmd.Execute())
	require.Contains(t, out.String(), "batches=")
	t.Log(out.String())

	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, after.Size(), before.Size(), "file should shrink")

	got, err := chkdb(path)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestCompactOnlineCommand_NoArgs(t *testing.T) {
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"compact-online"})
	err := rootCmd.Execute()
	require.Error(t, err)
}

func TestCompactOnlineCommand_MissingFile(t *testing.T) {
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"compact-online", filepath.Join(t.TempDir(), "does-not-exist")})
	err := rootCmd.Execute()
	require.Error(t, err)
}

var _ = mrand.Int
