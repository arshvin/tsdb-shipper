package cmds

import (
	"os"

	"github.com/go-kit/kit/log"
)

var logger log.Logger

func init() {
	logger = log.NewLogfmtLogger(os.Stdout)
}
