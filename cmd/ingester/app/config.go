package app

// Config is application config gathered from CLI arguments
type Config struct {
	DBDir              string
	WriteModeDB           bool
	Mint               int64
	Maxt               int64
	Partition          int64
	HumanReadable      bool
	ExtLabels          map[string]string
	URLStorage         string
	ConcurrentRequests uint8
	MaxSizePerRequest  int
	HTTPTimeout        int64
}

func Create() *Config {

	cfg := new(Config)
	cfg.ExtLabels = make(map[string]string)
	return cfg
}
