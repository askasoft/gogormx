package gormlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/askasoft/pango/log"
	"github.com/askasoft/pango/str"
	"github.com/askasoft/pango/tmu"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type GormLogger struct {
	Logger                   log.Logger
	DefaultSQLLevel          log.Level
	ErrorSQLLevel            log.Level
	WriteSQLLevel            log.Level
	SlowSQLLevel             log.Level
	SlowSQLTime              time.Duration
	TraceRecordNotFoundError bool
	ParameterizedQueries     bool
	GetErrLogLevel           func(error) log.Level
	GetSQLLogLevel           func(string) log.Level
	IsSlowSQL                func(string, time.Duration) bool
}

func NewGormLogger(logger log.Logger) *GormLogger {
	gl := &GormLogger{
		Logger:          logger,
		DefaultSQLLevel: log.LevelDebug,
		ErrorSQLLevel:   log.LevelError,
		WriteSQLLevel:   log.LevelInfo,
		SlowSQLLevel:    log.LevelWarn,
		SlowSQLTime:     2 * time.Second,
	}

	return gl
}

// LogMode log mode
func (gl *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	return gl
}

// Info print info
func (gl *GormLogger) Info(ctx context.Context, msg string, data ...any) {
	gl.printf(log.LevelInfo, msg, data...)
}

// Warn print warn messages
func (gl *GormLogger) Warn(ctx context.Context, msg string, data ...any) {
	gl.printf(log.LevelWarn, msg, data...)
}

// Error print error messages
func (gl *GormLogger) Error(ctx context.Context, msg string, data ...any) {
	gl.printf(log.LevelError, msg, data...)
}

// Trace print sql message
func (gl *GormLogger) ParamsFilter(ctx context.Context, sql string, params ...any) (string, []any) {
	if gl.ParameterizedQueries {
		return sql, nil
	}
	return sql, params
}

// Trace print sql message
func (gl *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	td := time.Since(begin)
	sql, rows := fc()

	switch {
	case err != nil && (!errors.Is(err, gorm.ErrRecordNotFound) || gl.TraceRecordNotFoundError):
		lvl := gl.ErrorSQLLevel
		if f := gl.GetErrLogLevel; f != nil {
			lvl = f(err)
		}
		if gl.Logger.IsLevelEnabled(lvl) {
			if rows < 0 {
				gl.printf(lvl, "%s [%s] %s", err, tmu.HumanDuration(td), sql)
			} else {
				gl.printf(lvl, "%s [%d: %s] %s", err, rows, tmu.HumanDuration(td), sql)
			}
		}
	case gl.isSlowSQL(sql, td):
		if rows < 0 {
			gl.printf(gl.SlowSQLLevel, "SLOW >= %s [%s] %s", tmu.HumanDuration(gl.SlowSQLTime), tmu.HumanDuration(td), sql)
		} else {
			gl.printf(gl.SlowSQLLevel, "SLOW >= %s [%d: %s] %s", tmu.HumanDuration(gl.SlowSQLTime), rows, tmu.HumanDuration(td), sql)
		}
	default:
		f := gl.GetSQLLogLevel
		if f == nil {
			f = gl.getSQLLogLevel
		}

		lvl := f(sql)
		if gl.Logger.IsLevelEnabled(lvl) {
			if rows < 0 {
				gl.printf(lvl, "[%v] %s", tmu.HumanDuration(td), sql)
			} else {
				gl.printf(lvl, "[%d: %v] %s", rows, tmu.HumanDuration(td), sql)
			}
		}
	}
}

func (gl *GormLogger) printf(lvl log.Level, msg string, data ...any) {
	if gl.Logger.IsLevelEnabled(lvl) {
		le := log.NewEvent(gl.Logger, lvl, fmt.Sprintf(msg, data...))
		le.CallerStop("gorm.io", gl.Logger.GetTraceLevel() >= lvl)
		gl.Logger.Write(le)
	}
}

func (gl *GormLogger) getSQLLogLevel(sql string) log.Level {
	sql = str.StripLeft(sql)
	if str.StartsWithFold(sql, "SELECT") {
		return gl.DefaultSQLLevel
	}
	return gl.WriteSQLLevel
}

func (gl *GormLogger) isSlowSQL(sql string, td time.Duration) bool {
	if gl.SlowSQLTime > 0 && td > gl.SlowSQLTime && gl.Logger.IsLevelEnabled(gl.SlowSQLLevel) {
		if f := gl.IsSlowSQL; f != nil {
			return f(sql, td)
		}

		return true
	}
	return false
}
