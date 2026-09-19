package command

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type onlineCompactOptions struct {
	batchPages    int
	shrinkTimeout time.Duration
}

func newOnlineCompactCommand() *cobra.Command {
	var o onlineCompactOptions
	cmd := &cobra.Command{
		Use:   "online-compact [options] <bbolt-file>",
		Short: "incrementally compacts a live database in place, relocating tail pages and shrinking the file without taking it offline.",
		Long: `online-compact performs online incremental compaction directly on the given
database file. Unlike "compact", it does not copy the database to a new file
and does not require exclusive/offline access: pages are relocated in a series
of short write transactions while concurrent readers and (for an already open
database) writers keep being served.

The logical high water mark is lowered and the free file tail is truncated. If
long-lived read transactions are still open when the physical shrink is due,
the run relocates all pages but defers the truncation; rerun the command after
the readers close to reclaim the file space.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.Run(cmd, args[0])
		},
	}
	o.AddFlags(cmd.Flags())
	return cmd
}

func (o *onlineCompactOptions) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&o.batchPages, "batch-pages", bolt.DefaultOnlineCompactBatchPages, "maximum number of tail pages relocated per write transaction")
	fs.DurationVar(&o.shrinkTimeout, "shrink-timeout", 30*time.Second, "max time to wait for long-lived read transactions before truncating the file tail; negative defers the truncation")
}

func (o *onlineCompactOptions) Run(cmd *cobra.Command, srcPath string) error {
	fi, err := checkSourceDBPath(srcPath)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	db, err := bolt.Open(srcPath, fi.Mode(), nil)
	if err != nil {
		return err
	}
	defer db.Close()

	stats, err := db.OnlineCompact(&bolt.OnlineCompactOptions{
		BatchPages:    o.batchPages,
		ShrinkTimeout: o.shrinkTimeout,
	})
	if err != nil {
		return err
	}

	finalFi, err := checkSourceDBPath(srcPath)
	if err != nil {
		return err
	}
	gain := 1.0
	if finalFi.Size() > 0 {
		gain = float64(initialSize) / float64(finalFi.Size())
	}
	fmt.Fprintf(cmd.OutOrStdout(),
		"batches=%d moved-pages=%d hwm=%d->%d %d -> %d bytes (gain=%.2fx) shrunk=%t\n",
		stats.Batches, stats.MovedPages, stats.HighWaterBefore, stats.HighWaterAfter,
		initialSize, finalFi.Size(), gain, stats.Shrunk)
	if stats.ShrinkDeferred {
		fmt.Fprintln(cmd.OutOrStdout(), "physical shrink deferred: rerun after long-lived read transactions close")
	}
	return nil
}
