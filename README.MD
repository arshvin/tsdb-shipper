# Useless tool
This is a tool that allows you to ship Prometheus metrics which are stored in some directory to Victoria Metrics server using "Prometheus remote write" protocol. Therefore it helps not to lose previously collected metrics by Prometheus if you didn't use Remote write storage from start.

The tool has 2 main commands:
* `ls` that is analog of `ls` command of Prometheus' tsdb. This command is useful to inspect of directory containing OpenTSDB block and Wal.
* `ship` that is able to read blocks and Wal from specified directory and send them to remote URL.

## How to use
Build command:
```bash
go build tsdb-shipper/cmd/ingester
```
Check the result:
```bash
./ingester --help
usage: Ingerter [<flags>] <command> [<args> ...]

Ingerter of openTsdb historical time series data for Victoria-metrics server

Flags:
  --help  Show context-sensitive help (also try --help-long and --help-man).

Commands:
  help [<command>...]
    Show help.

  ship [<flags>]
    Send metrics to remote storage

  ls [<flags>]
    List blocks in scpecified directory
```
