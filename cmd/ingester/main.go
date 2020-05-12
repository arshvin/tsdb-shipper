package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	logger           log.Logger
	remoteStorageURL string
	tsDbDir          string
	externalLabels   map[string]string
	mint             int64
	maxt             int64
	tmpDir           string
)

const (
	httpTimeout             = 30 * time.Second
	maxTimeSeriesPerRequest = 10
	concurrency             = 4
)

func init() {
	logger = log.NewLogfmtLogger(os.Stdout)
	externalLabels = make(map[string]string)
}

func main() {
	parseFlag()

	db, err := tsdb.OpenDBReadOnly(tsDbDir, nil)
	if err != nil {
		logger.Log("stage", "openning storage", "error", err)
		os.Exit(1)
	}

	querier, err := db.Querier(mint, maxt)
	if err != nil {
		logger.Log("stage", "getting the querier", "error", err)
		os.Exit(1)
	}

	seriesSets, err := querier.Select(labels.MustNewMatcher(labels.MatchRegexp, "", ".*"))
	if err != nil {
		logger.Log("stage", "selecting metrics", "error", err)
		os.Exit(1)
	}

	totalSeries := uint64(0)
	totalSamples := uint64(0)
	defer func() {
		logger.Log("stage", "finished", "series-number", totalSeries, "samples-number", totalSamples)
	}()

	data := make(chan []byte, concurrency+1)
	defer close(data)

	for i := 0; i < concurrency; i++ {
		go sendPreparedTimeSeries(data)
	}

	protoTSList := []prompb.TimeSeries{}

	for seriesSets.Next() {
		series := seriesSets.At()

		protoTS, processedSamples := seriesToProto(series)
		protoTSList = append(protoTSList, protoTS)

		totalSamples += processedSamples
		totalSeries++

		if totalSeries % maxTimeSeriesPerRequest == 0 {
			logger.Log("stage", "shipping metrics", "total-time-series", totalSeries, "total-samples", totalSamples)
			data <- makeProtoRequest(protoTSList)
			protoTSList = make([]prompb.TimeSeries, 0)
		}
	}

	data <- makeProtoRequest(protoTSList)
}

func makeProtoRequest(protoTimeSeries []prompb.TimeSeries) []byte {
	protoReq := prompb.WriteRequest{}
	protoReq.Timeseries = protoTimeSeries

	pBufData, err := proto.Marshal(&protoReq)
	if err != nil {
		logger.Log("stage", "protobuf message encoding", "error", err)
		os.Exit(1)
	}

	return snappy.Encode(pBufData)
}

func parseFlag() {
	app := kingpin.New("Ingester", "Ingerter historical time series data into openTsdb")

	app.Flag("url", "Remote addres of the destionatoin storage").Default("").
		StringVar(&remoteStorageURL)

	app.Flag("blocks-dir", "Source of TSDB storage").Default("").
		StringVar(&tsDbDir)

	app.Flag("external-label", "Label that will be added to each series from this source").
		PlaceHolder("name=value").StringMapVar(&externalLabels)

	app.Flag("min-time", "Left edge of time range").Default(strconv.FormatInt(math.MinInt64, 10)).
		Int64Var(&mint)

	app.Flag("max-time", "Right edge of time range").
		Default(strconv.FormatInt(time.Now().Local().Unix()*1000, 10)).
		Int64Var(&maxt)

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		logger.Log("stage", "parsing cli flags", "error", err)
		os.Exit(1)
	}
}

func seriesToProto(series tsdb.Series) (protoTimeSeries prompb.TimeSeries, samplesCount uint64) {

	labels := series.Labels()
	it := series.Iterator()

	//ExternalLabels
	for k, v := range externalLabels {
		protoLabel := prompb.Label{Name: k, Value: v}
		protoTimeSeries.Labels = append(protoTimeSeries.Labels, protoLabel)
	}

	//Construct the Labels part
	for _, label := range labels {
		protoLabel := prompb.Label{Name: label.Name, Value: label.Value}
		protoTimeSeries.Labels = append(protoTimeSeries.Labels, protoLabel)
	}

	for it.Next() {
		t, v := it.At()
		protoSample := prompb.Sample{Timestamp: t, Value: v}
		protoTimeSeries.Samples = append(protoTimeSeries.Samples, protoSample)

		samplesCount++
	}

	return
}

func sendPreparedTimeSeries(input chan []byte) {
	writeErrorAndExit := func(err error) {
		logger.Log("stage", "sending write request", "error", err)
		os.Exit(1)
	}

	u, err := url.Parse(remoteStorageURL)
	if err != nil {
		writeErrorAndExit(err)
	}

	for {
		select {
		case data, opened := <-input:
			if !opened {
				logger.Log("stage", "sending write request", "status", "goroutine finished")
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)

			req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer(data))
			if err != nil {
				writeErrorAndExit(err)
			}

			req.Header.Add("Content-Encoding", "snappy")
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("User-Agent", "go-cli")
			req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

			resp, err := http.DefaultClient.Do(req.WithContext(ctx))
			if err != nil {
				writeErrorAndExit(err)
			}

			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusNoContent {
				writeErrorAndExit(fmt.Errorf("http code %d was returned", resp.StatusCode))
			}

			cancel() // Release context resources
		}
	}
}
