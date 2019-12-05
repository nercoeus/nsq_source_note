package nsqd

import (
	"github.com/nsqio/nsq/internal/lg"
)

type Logger lg.Logger

// 五种日志等级
const (
	LOG_DEBUG = lg.DEBUG
	LOG_INFO  = lg.INFO
	LOG_WARN  = lg.WARN
	LOG_ERROR = lg.ERROR
	LOG_FATAL = lg.FATAL
)

// 打印日志
func (n *NSQD) logf(level lg.LogLevel, f string, args ...interface{}) {
	opts := n.getOpts()
	lg.Logf(opts.Logger, opts.LogLevel, level, f, args...)
}
