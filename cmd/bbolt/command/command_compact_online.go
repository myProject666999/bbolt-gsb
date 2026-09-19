package command

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type compactOnlineOptions struct {
	pagesPerBatch int
	maxBatches    int
	shrinkTimeout time.Duration
	disableShrink bool
	dstNoSync     bool
}

func newCompactOnlineCommand() *cobra.Command {
	var o compactOnlineOptions
	cmd := &cobra.Command{
		Use:   "compact-online [options] <bbolt-file>",
		Short: "incrementally compact and shrink a database in place while preserving its file",
		Long: `compact-online performs online incremental compaction of a single database file.

It relocates live pages in small batches, each committed as an ordinary
transaction, and then truncates the reclaimed file tail. Unlike "compact" it
does not copy the database to a new destination file; the given file is
rewritten in place.

The database is opened read-write (an exclusive flock is taken, as for any
writer), but the relocation batches use the normal transaction machinery, so
the algorithm is designed for databases that remain in service in-process.
The on-disk format is unchanged and remains readable by older bbolt versions.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.Run(cmd, args[0])
		},
	}
	o.AddFlags(cmd.Flags())
	return cmd
}

func (o *compactOnlineOptions) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&o.pagesPerBatch, "pages-per-batch", bolt.DefaultOnlineCompactBatchPages,
		"maximum number of live pages relocated per batch transaction")
	fs.IntVar(&o.maxBatches, "max-batches", 0,
		"maximum number of relocation batches (0 means run until done)")
	fs.DurationVar(&o.shrinkTimeout, "shrink-timeout", 30*time.Second,
		"maximum time to wait for open read-only transactions before shrinking")
	fs.BoolVar(&o.disableShrink, "no-shrink", false,
		"relocate pages but do not truncate the data file")
	fs.BoolVar(&o.dstNoSync, "no-sync", false, "skip fsync when committing batches")
}

func (o *compactOnlineOptions) Run(cmd *cobra.Command, dbPath string) error {
	fi, err := checkSourceDBPath(dbPath)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	db, err := bolt.Open(dbPath, fi.Mode(), &bolt.Options{NoSync: o.dstNoSync})
	if err != nil {
		return err
	}
	defer db.Close()

	stats, err := db.CompactOnline(&bolt.OnlineCompactConfig{
		MaxPagesPerBatch: o.pagesPerBatch,
		MaxBatches:       o.maxBatches,
		ShrinkTimeout:    o.shrinkTimeout,
		DisableShrink:    o.disableShrink,
	})
	if err != nil {
		return err
	}

	finalFi, err := os.Stat(dbPath)
	if err != nil {
		return err
	}
	gain := 1.0
	if finalFi.Size() > 0 {
		gain = float64(initialSize) / float64(finalFi.Size())
	}
	fmt.Fprintf(cmd.OutOrStdout(),
		"%d -> %d bytes (gain=%.2fx) batches=%d pages_moved=%d shrinks=%d freed=%d\n",
		initialSize, finalFi.Size(), gain, stats.Batches, stats.PagesMoved, stats.Shrinks, stats.FreedBytes)
	return nil
}
