package cmds

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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"

	"tsdb-shipper/cmd/ingester/app"
	"tsdb-shipper/cmd/ingester/db"

	"github.com/prometheus/prometheus/tsdb"
)

type fetchedData struct {
	series  tsdb.SeriesSet
	querier tsdb.Querier
}

// ShipMetrics transfers metrics from local dir to remote storage with help protobuf
func ShipMetrics(db *db.DB, cfg *app.Config) {

	logger := log.With(logger, "stage", "shipMetrics")

	output := make(chan *fetchedData)

	pipeline := make(chan []byte, cfg.ConcurrentRequests+1)
	go processTimeSeriesSet(output, pipeline, cfg.MaxSizePerRequest, cfg.ExtLabels)

	var wg sync.WaitGroup
	for i := 0; i < int(cfg.ConcurrentRequests); i++ {
		wg.Add(1)
		go sendPreparedTimeSeries(&wg, pipeline, cfg.URLStorage, cfg.HTTPTimeout)
	}

	if cfg.Partition <= 0 {
		cfg.Partition = 1
	}

	delta := cfg.Partition * 1000 // Seconds -> Milliseconds
	mint, maxt := checkTimeRange(db, cfg.Mint, cfg.Maxt)

	logger.Log("from", mint, "to", maxt)

	for current := mint; current <= maxt; current += delta {
		output <- getSeriesSetFor(db, current, current+delta)
	}

	close(output)
	logger.Log("status", "waiting for finish senders")

	wg.Wait()
	logger.Log("status", "finished")
}

//This function is for shrinking specified in CLI time range to existing data time range
func checkTimeRange(db *db.DB, mint, maxt int64) (int64, int64) {

	logger := log.With(logger, "stage", "checkTimeRange")

	var actualMint int64 = math.MaxInt64
	var actualMaxt int64 = math.MinInt64

	if db.ReadOnly {
		blocks, _ := db.BlockReader()
		//Look time ranges for blocks
		for _, block := range blocks {
			if actualMint > block.Meta().MinTime {
				actualMint = block.Meta().MinTime
			}

			if actualMaxt < block.Meta().MaxTime {
				actualMaxt = block.Meta().MaxTime
			}
		}
	} else {
		blocks := db.Blocks()
		//Look time ranges for blocks
		for _, block := range blocks {
			if actualMint > block.Meta().MinTime {
				actualMint = block.Meta().MinTime
			}

			if actualMaxt < block.Meta().MaxTime {
				actualMaxt = block.Meta().MaxTime
			}
		}
	}

	logger.Log("status", fmt.Sprintf("According to Blocks Mint: %d, Maxt: %d", actualMint, actualMaxt))

	head := db.Head() // If tsdb opened in write mode
	if head != nil {
		//Look the time range for head
		if head.MinTime() < actualMint {
			actualMint = head.MinTime()
		}

		if head.MaxTime() > actualMaxt {
			actualMaxt = head.MaxTime()
		}

		logger.Log("status", fmt.Sprintf("According to Head Mint: %d, Maxt: %d", actualMint, actualMaxt))
	}

	if actualMint < mint {
		actualMint = mint
	} // else use calculated actualMint

	if actualMaxt > maxt {
		actualMaxt = maxt
	} // else use calculated actualMaxt

	return actualMint, actualMaxt

}

func getSeriesSetFor(db *db.DB, mint, maxt int64) *fetchedData {
	logger := log.With(logger, "stage", "getSeriesSetFor")

	logger.Log("status", "getting querier for range", "from", mint, "to", maxt)

	querier, err := db.Querier(mint, maxt)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	matcher := labels.MustNewMatcher(labels.MatchRegexp, "", ".*")
	seriesSet, err := querier.Select(matcher)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	return &fetchedData{
		series:  seriesSet,
		querier: querier,
	}
}

func processTimeSeriesSet(input chan *fetchedData, output chan []byte, maxTSeriesPerRequest int, extLabels map[string]string) {

	defer close(output)
	logger := log.With(logger, "stage", "processTimeSeriesSet")

	totalSeries := uint64(0)
	totalSamples := uint64(0)

	protoTSList := []prompb.TimeSeries{}

	batchSize := 0
	for fetchedData := range input {
		for fetchedData.series.Next() {
			series := fetchedData.series.At()

			protoTS, processedSamples := seriesToProto(series, extLabels)
			protoTSList = append(protoTSList, protoTS)

			totalSamples += processedSamples
			totalSeries++
			batchSize += getProtoTSSize(protoTS)

			if batchSize > maxTSeriesPerRequest {
				output <- makeProtoRequest(protoTSList)
				protoTSList = make([]prompb.TimeSeries, 0)
			}
		}

		logger.Log("status", "closing querier for range", "total-time-series", totalSeries, "total-samples", totalSamples)
		fetchedData.querier.Close()
	}

	output <- makeProtoRequest(protoTSList)
}

func makeProtoRequest(protoTimeSeries []prompb.TimeSeries) []byte {
	logger := log.With(logger, "stage", "makeProtoRequest")

	protoReq := prompb.WriteRequest{}
	protoReq.Timeseries = protoTimeSeries

	pBufData, err := proto.Marshal(&protoReq)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	return snappy.Encode(nil, pBufData)
}

func seriesToProto(series tsdb.Series, extLabels map[string]string) (protoTimeSeries prompb.TimeSeries, samplesCount uint64) {

	it := series.Iterator()

	//ExternalLabels
	for k, v := range extLabels {
		protoLabel := prompb.Label{Name: k, Value: v}
		protoTimeSeries.Labels = append(protoTimeSeries.Labels, protoLabel)
	}

	//Construct the Labels part
	for _, label := range series.Labels() {
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

func getProtoTSSize(ts prompb.TimeSeries) int {
	return ts.Size()
}

func sendPreparedTimeSeries(wg *sync.WaitGroup, input chan []byte, urlStorage string, timeout int64) {
	logger := log.With(logger, "stage", "sendPreparedTimeSeries")

	writeErrorAndExit := func(err error) {
		logger.Log("error", err)
		os.Exit(1)
	}

	u, err := url.Parse(urlStorage)
	if err != nil {
		writeErrorAndExit(err)
	}

	for {
		select {
		case data, opened := <-input:
			if !opened {
				logger.Log("status", "goroutine finished")
				wg.Done()
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout))

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
