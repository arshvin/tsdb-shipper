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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/prometheus/prometheus/tsdb"
)

type fetchedData struct {
	series tsdb.SeriesSet
	querier   tsdb.Querier
}

func shipMetrics(db *tsdb.DBReadOnly, mint, maxt, partition int64) {

	logger := log.With(logger,"stage","shipMetrics")
	
	output := make(chan *fetchedData)
	defer close(output)

	go processTimeSeriesSet(output)

	if partition <= 0 {
		partition = 1
	}
	delta := partition * int64(time.Microsecond)

	mint, maxt = checkTimeRange(db, mint, maxt)

	logger.Log("from", mint, "to", maxt)

	for current := mint; current <= maxt; current += delta {
		output <- getSeriesSetFor(db, current, current+delta)
	}


}

//This function is for shrinking specified in CLI time range to existing data time range
func checkTimeRange(db *tsdb.DBReadOnly, mint, maxt int64) (int64, int64) {
	logger := log.With(logger,"stage","checkTimeRange")

	blocks, err := db.Blocks()
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	var actualMint int64 = math.MaxInt64
	var actualMaxt int64 = math.MinInt64

	for _, block := range blocks {
		if actualMint > block.Meta().MinTime {
			actualMint = block.Meta().MinTime
		}

		if actualMaxt < block.Meta().MaxTime {
			actualMaxt = block.Meta().MaxTime
		}
	}

	if actualMint < mint {
		actualMint = mint
	} // else use calculated actualMint

	if actualMaxt > maxt {
		actualMaxt = maxt
	} // else use calculated actualMaxt

	return actualMint, actualMaxt

}

func getSeriesSetFor(db *tsdb.DBReadOnly, mint, maxt int64) *fetchedData {
	logger := log.With(logger,"stage","getSeriesSetFor")
	
	logger.Log("status","getting querier for range","from", mint, "to", maxt)

	querier, err := db.Querier(mint, maxt)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	// logger.Log("from", time.Unix(mint/1000, 0).String(), "to", time.Unix(maxt/1000, 0).String())
	// logger.Log("from", mint, "to", maxt)

	matcher := labels.MustNewMatcher(labels.MatchRegexp, "", ".*")
	seriesSet, err := querier.Select(matcher)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	return &fetchedData{
		series: seriesSet,
		querier:   querier,
	}
}

func processTimeSeriesSet(input chan *fetchedData) {
	logger := log.With(logger,"stage","processTimeSeriesSet")

	totalSeries := uint64(0)
	totalSamples := uint64(0)

	defer func() {
		logger.Log("status","finished","total-uploaded-series-number", totalSeries, "total-uploaded-samples-number", totalSamples)
	}()

	output := make(chan []byte, concurrentRequests+1)
	defer close(output)

	for i := 0; i < concurrentRequests; i++ {
		go sendPreparedTimeSeries(output)
	}

	protoTSList := []prompb.TimeSeries{}

	for fetchedData := range input {
		
		for fetchedData.series.Next() {
			series := fetchedData.series.At()

			protoTS, processedSamples := seriesToProto(series)
			protoTSList = append(protoTSList, protoTS)

			totalSamples += processedSamples
			totalSeries++

			if totalSeries%maxTSeriesPerRequest == 0 {
				output <- makeProtoRequest(protoTSList)
				protoTSList = make([]prompb.TimeSeries, 0)
			}
		}
		// logger.Log( "total-time-series", totalSeries, "total-samples", totalSamples)
		
		logger.Log("status","closing querier for range","total-time-series", totalSeries, "total-samples", totalSamples)
		fetchedData.querier.Close()

	}

	output <- makeProtoRequest(protoTSList)
}

func makeProtoRequest(protoTimeSeries []prompb.TimeSeries) []byte {
	logger := log.With(logger,"stage","makeProtoRequest")

	protoReq := prompb.WriteRequest{}
	protoReq.Timeseries = protoTimeSeries

	pBufData, err := proto.Marshal(&protoReq)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}

	return snappy.Encode(nil, pBufData)
}

func seriesToProto(series tsdb.Series) (protoTimeSeries prompb.TimeSeries, samplesCount uint64) {

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

func sendPreparedTimeSeries(input chan []byte) {
	logger := log.With(logger,"stage","sendPreparedTimeSeries")

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
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(httpTimeout))

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
