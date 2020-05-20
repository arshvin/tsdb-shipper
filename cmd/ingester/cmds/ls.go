package cmds

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"
	"tsdb-shipper/cmd/ingester/db"

	"github.com/go-kit/kit/log"
)

// PrintBlocksInfo list information about existing blocks
func PrintBlocksInfo(db *db.DB, humanReadable *bool) {
	logger := log.With(logger, "stage", "PrintBlocksInfo")

	blocks, err := db.BlockReader()
	if err != nil {
		logger.Log("error", err)
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
