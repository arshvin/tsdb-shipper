package cmds

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	"tsdb-shipper/cmd/ingester/db"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/tsdb/testutil"
)

func TestCheckTimeRange(t *testing.T) {
	//Create new test DB that will be removed after finish
	db, tmpDir := openTestDB(t)
	defer closeTestDB(db, tmpDir)

	lb := new(labels.Builder)
	lb.Set("test", "TestCheckTimeRange")
	lb.Set("environment", "test")
	labels := lb.Labels()

	t1 := time.Now()
	t2 := t1.Add((-10) * time.Second) //-10 seconds from now

	app := db.Appender()
	for tc := t1.Nanosecond(); tc >= t2.Nanosecond(); tc-- {
		app.Add(labels, int64(tc), float64(tc))
	}

	err := app.Commit()
	if err != nil {
		t.Fatal("Could not commit test data to DB")
	}

	// Start testing
	mint, maxt := checkTimeRange(db, int64(t2.Nanosecond()), int64(t1.Nanosecond()))

	if mint != int64(t2.Nanosecond()) {
		t.Errorf("Expected mint: %d. Actual: %d", mint, int64(t2.Nanosecond()))
	}

	if maxt != int64(t1.Nanosecond()) {
		t.Errorf("Expected maxt: %d. Actual: %d", maxt, int64(t1.Nanosecond()))
	}

	t3 := t1.Add((-20) * time.Second) //-20 seconds from now
	t4 := t1.Add((-20) * time.Second) //-5 seconds from now

	mint, maxt = checkTimeRange(db, int64(t3.Nanosecond()), int64(t4.Nanosecond()))

	if mint != int64(t2.Nanosecond()) {
		t.Errorf("Expected mint: %d. Actual: %d", mint, int64(t2.Nanosecond()))
	}

	if maxt != int64(t4.Nanosecond()) {
		t.Errorf("Expected maxt: %d. Actual: %d", maxt, int64(t4.Nanosecond()))
	}
}

// func TestGetProtoTSSize(t *testing.T) {

// 	lb := new(labels.Builder)
// 	lb.Set("test", "TestGetSeriesSetFor")
// 	lb.Set("environment", "test")
// 	labels := lb.Labels()

// 	metricSize := 0
// 	ts := tsdb.Series

// 	n := 10
// 	for tsCount := 0; n < 10; tsCount++ {
// 		t := int64(time.Now().Unix())
// 		v := float64(1.0)

// 		metricSize += int(unsafe.Sizeof(t))
// 		metricSize += int(unsafe.Sizeof(t))
// 	}

// 	t2 := time.Now()

// 	labelSize := 0
// 	for _, label := range labels {
// 		labelSize += len(label.Name)
// 		labelSize += len(label.Value)
// 	}

// 	expectedSize := metricSize + (labelSize * n)

// 	//Gathering saved metrics

// 	data := getSeriesSetFor(db, t1.Unix(), t2.Unix())
// 	actualSize := 0
// 	for data.series.Next() {
// 		protoTS, _ := seriesToProto(data.series.At())
// 		actualSize += getProtoTSSize(protoTS)
// 	}

// 	if actualSize != expectedSize {
// 		t.Errorf("Expected size: %d. Actual size: %d", expectedSize, actualSize)
// 	}
// }

func TestGetProtoTSSize(t *testing.T) {
	db, tmpDir := openTestDB(t)
	defer closeTestDB(db, tmpDir)

	lb := new(labels.Builder)
	lb.Set("test", "TestGetProtoTSSize")
	lb.Set("environment", "test")
	labels := lb.Labels()

	n := 10
	t1 := time.Now()
	t2 := t1.Add(time.Duration(n) * time.Second)

	logger = log.NewLogfmtLogger(os.Stdout)

	metricSize := 0
	app := db.Appender()

	for tc := t1.Unix(); tc < t2.Unix(); tc ++ {
		tt := int64(tc * 1000)
		tv := float64(1.0)

		_, err := app.Add(labels, tt, tv)
		testutil.Ok(t, err)

		metricSize += int(unsafe.Sizeof(tt))
		metricSize += int(unsafe.Sizeof(tv))

		logger.Log("timeSize", int(unsafe.Sizeof(tt)),"valuesSize",int(unsafe.Sizeof(tv)))
	}

	err := app.Commit()
	testutil.Ok(t, err)

	labelSize := 0
	for _, label := range labels {
		labelSize += len(label.Name)
		labelSize += len(label.Value)
		logger.Log("label.Name", len(label.Name),"abel.Value",len(label.Value))
	}

	expectedSize := metricSize + labelSize

	//Gathering saved metrics
	actualSize := 0
	extLabels := make(map[string]string)
	var sampleCounter int

	data := getSeriesSetFor(db, t1.Unix()*1000, t2.Unix()*1000)
	ss := data.series

	for ss.Next() {
		protoTS, samples := seriesToProto(ss.At(), extLabels)
		actualSize += getProtoTSSize(protoTS)
		sampleCounter += int(samples)
	}

	if sampleCounter != n {
		t.Errorf("Not all samples were returned! Expected: %d, Actual: %d", n, sampleCounter)
	}

	if actualSize != expectedSize {
		t.Errorf("Expected size: %d. Actual size: %d", expectedSize, actualSize)
	}
}

func openTestDB(t *testing.T) (*db.DB, string) {
	tmpDir, err := ioutil.TempDir("", "testdb")

	testutil.Ok(t, err)

	return db.Open(&tmpDir, false), tmpDir
}

func closeTestDB(db *db.DB, tmpDir string) {
	db.Close()
	os.RemoveAll(tmpDir)
}
