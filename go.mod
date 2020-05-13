module tsdb-shipper

go 1.13

require (
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/prometheus/common v0.8.0
	github.com/prometheus/prometheus v1.8.2-0.20200213233353-b90be6f32a33
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

// replace github.com/prometheus/prometheus => /Users/arshvin/public_repos/prometheus
