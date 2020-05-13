package main

import (
	"math"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/prometheus/prometheus/tsdb"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logger log.Logger

	extLabels map[string]string

	urlStorage           string
	concurrentRequests   int
	maxTSeriesPerRequest uint64
	httpTimeout          int64
)

const ()

func init() {
	logger = log.NewLogfmtLogger(os.Stdout)
}

func main() {
	parseFlag()
}

func parseFlag() {
	var (
		tsDBDir       string
		mint          int64
		maxt          int64
		partition     int64
		humanReadable bool
	)

	extLabels = make(map[string]string)

	app := kingpin.New("Ingerter", "Ingerter of openTsdb historical time series data for Victoria-metrics server")

	shipCmd := app.Command("ship", "Send metrics to remote storage")
	shipCmd.Flag("url", "Remote addres of the destination storage").
		Short('u').
		Default("").
		StringVar(&urlStorage)

	shipCmd.Flag("source", "Source directory of TSDB storage").
		Short('s').
		Default("").
		StringVar(&tsDBDir)

	shipCmd.Flag("external-label", "Label to add to each series from specified source").
		Short('l').
		PlaceHolder("name=value").
		StringMapVar(&extLabels)

	shipCmd.Flag("min-time", "Left edge of time range").
		Short('b').
		Default(strconv.FormatInt(math.MinInt64, 10)).
		Int64Var(&mint)

	shipCmd.Flag("max-time", "Right edge of time range").
		Short('e').
		Default(strconv.FormatInt(time.Now().Local().Unix()*1000, 10)).
		Int64Var(&maxt)

	shipCmd.Flag("split-time", "Amount second for partition source timeseries").
		Short('p').
		Default("3600").
		Int64Var(&partition)

	shipCmd.Flag("http-concurrency", "Amount concurrent http-request").
		Default("5").
		IntVar(&concurrentRequests)

	shipCmd.Flag("http-timeout", "Timeout seconds of http-request").
		Default("30").
		Int64Var(&httpTimeout)

	shipCmd.Flag("ts-per-request", "Amount of sending timeseries per http-request").
		Default("10").
		Uint64Var(&maxTSeriesPerRequest)

	lsCmd := app.Command("ls", "List blocks in scpecified directory")
	lsCmd.Flag("source", "Source directory of TSDB storage").
		Short('s').
		Default("").
		StringVar(&tsDBDir)

	lsCmd.Flag("human-readable", "Print dates in human view").Short('h').
		BoolVar(&humanReadable)

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case lsCmd.FullCommand():
		db := openTsdb(&tsDBDir)

		printBlocksInfo(db, &humanReadable)

	case shipCmd.FullCommand():
		db := openTsdb(&tsDBDir)
		httpTimeout = httpTimeout * int64(time.Second)

		shipMetrics(db, mint, maxt, partition)
	}
}

func openTsdb(path *string) *tsdb.DBReadOnly {
	logger := log.With(logger, "stage", "openTsdb")
	db, err := tsdb.OpenDBReadOnly(*path, logger)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}
	logger.Log("status", "TSDB opened successfully")
	return db
}