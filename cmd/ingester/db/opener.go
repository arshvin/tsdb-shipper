package db

import (
	"os"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/tsdb"
)

var logger log.Logger

func init() {
	logger = log.NewLogfmtLogger(os.Stdout)
}

// DB is the abctraction for usage of openTsDb whether write or readonly modes
type DB struct {
	readOnlyDB *tsdb.DBReadOnly
	writeDB    *tsdb.DB
	ReadOnly   bool
}

// Open is factory function returning opened openTsDB instanse through DB-abstraction
func Open(path *string, readOnly bool) *DB {
	var db *DB = new(DB)
	db.ReadOnly = readOnly

	if readOnly {
		db.readOnlyDB = openTsDBReadOnly(path)
	} else {
		db.writeDB = openTsDB(path)
	}
	return db
}

func openTsDB(path *string) *tsdb.DB {
	logger := log.With(logger, "stage", "openTsDB for write mode")

	options := tsdb.DefaultOptions
	options.WALCompression = false
	options.NoLockfile = true
	options.RetentionDuration = 365 * 24 * 60 * 60 * 1000 // 365 days in milliseconds

	db, err := tsdb.Open(
		*path, logger, nil, options)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}
	logger.Log("status", "TSDB opened successfully")

	db.DisableCompactions()
	
	return db
}

func openTsDBReadOnly(path *string) *tsdb.DBReadOnly {
	logger := log.With(logger, "stage", "openTsDB for read-only mode")

	db, err := tsdb.OpenDBReadOnly(*path, logger)
	if err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}
	logger.Log("status", "TSDB opened successfully")
	return db
}

// Querier returns the Querier instance
func (db *DB) Querier(mint, maxt int64) (tsdb.Querier, error) {
	if db.ReadOnly {
		return db.readOnlyDB.Querier(mint, maxt)
	}
	return db.writeDB.Querier(mint, maxt)
}

// Close closes the opened  database
func (db *DB) Close() error {
	if db.ReadOnly {
		return db.readOnlyDB.Close()
	}
	return db.writeDB.Close()
}

// BlockReader returns blocks for read-only tsdb instance
func (db *DB) BlockReader() ([]tsdb.BlockReader, error) {
	if db.ReadOnly {
		return db.readOnlyDB.Blocks()
	}
	return nil, nil
}

// Blocks returns blocks for read-only tsdb instance
func (db *DB) Blocks() []*tsdb.Block {
	if !db.ReadOnly {
		return db.writeDB.Blocks()
	}
	return nil
}

// Appender return appender for write-open tsdb instance else nil
func (db *DB) Appender() tsdb.Appender {
	if !db.ReadOnly {
		return db.writeDB.Appender()
	}
	return nil
}

//Head return head of write-open tsdb instance else nil
func (db *DB) Head() *tsdb.Head {
	if !db.ReadOnly {
		return db.writeDB.Head()
	}
	return nil
}
