package main

import (
	"math"
	"os"
	"strconv"
	"time"
	appConfig "tsdb-shipper/cmd/ingester/app"
	"tsdb-shipper/cmd/ingester/cmds"
	"tsdb-shipper/cmd/ingester/db"

	"github.com/go-kit/kit/log"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logger log.Logger
)

func init() {
	logger = log.NewLogfmtLogger(os.Stdout)
}

func main() {
	parseFlag()
}

func parseFlag() {

	config := appConfig.Create()

	app := kingpin.New("Ingerter", "Ingerter of openTsdb historical time series data for Victoria-metrics server")

	shipCmd := app.Command("ship", "Send metrics to remote storage")
	shipCmd.Flag("url", "Remote addres of the destination storage").
		Short('u').
		Default("").
		StringVar(&config.URLStorage)

	shipCmd.Flag("source", "Source directory of TSDB storage").
		Short('s').
		Default("").
		StringVar(&config.DBDir)

	shipCmd.Flag("external-label", "Label to add to each series from specified source").
		Short('l').
		PlaceHolder("name=value").
		StringMapVar(&config.ExtLabels)

	shipCmd.Flag("min-time", "Left edge of time range").
		Short('b').
		Default(strconv.FormatInt(math.MinInt64, 10)).
		Int64Var(&config.Mint)

	shipCmd.Flag("max-time", "Right edge of time range").
		Short('e').
		Default(strconv.FormatInt(time.Now().Local().Unix()*1000, 10)).
		Int64Var(&config.Maxt)

	shipCmd.Flag("split-time", "Amount second for partition source timeseries").
		Short('p').
		Default("3600").
		Int64Var(&config.Partition)

	shipCmd.Flag("http-concurrency", "Amount concurrent http-request").
		Default("5").
		Uint8Var(&config.ConcurrentRequests)

	shipCmd.Flag("http-timeout", "Timeout seconds of http-request").
		Default("30").
		Int64Var(&config.HTTPTimeout)

	shipCmd.Flag("send-max-size", "Approximate max size for http-request in MB").
		Default("10").
		IntVar(&config.MaxSizePerRequest)

	shipCmd.Flag("read-write", "Open TsDB in read-write mode. Defailt: read-only").
		Default("false").
		BoolVar(&config.WriteModeDB)


	lsCmd := app.Command("ls", "List blocks in scpecified directory")
	lsCmd.Flag("source", "Source directory of TSDB storage").
		Short('s').
		Default("").
		StringVar(&config.DBDir)

	lsCmd.Flag("human-readable", "Print dates in human view").Short('h').
		BoolVar(&config.HumanReadable)

	switch kingpin.MustParse(app.Parse(os.Args[1:])) {

	case lsCmd.FullCommand():
		db := db.Open(&config.DBDir, true)

		cmds.PrintBlocksInfo(db, &config.HumanReadable)

	case shipCmd.FullCommand():
		db := db.Open(&config.DBDir, !config.WriteModeDB)

		config.HTTPTimeout = config.HTTPTimeout * int64(time.Second)
		config.MaxSizePerRequest = config.MaxSizePerRequest * 1000 * 1000
		cmds.ShipMetrics(db, config)
	}
}
