package main

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/prometheus/prometheus/tsdb"

)

func printBlocksInfo(db *tsdb.DBReadOnly, humanReadable *bool) {

	blocks, err := db.Blocks()
	if err != nil {
		logger.Log("stage", "getting blocks", "error", err)
		os.Exit(1)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable *bool) string {
	if *humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}
