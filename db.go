package glib

import (
	"context"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"

	"github.com/micro/go-log"
)

// DBConfig is config for struct
type DBConfig struct {
	Enable   bool          `json:"enable"`
	ShowSql  bool          `json:"showSql"`
	LogLevel string        `json:"logLevel"`
	Alias    string        `json:"alias"`
	Driver   string        `json:"driver"`
	Dsn      []string      `json:"dsn"`
	TTL      time.Duration `json:"ttl"`
}

var dbs sync.Map

// DB will return a instance of `xorm.EngineGroup`, panic if it's not exists
func DB(alias string) *xorm.EngineGroup {

	eg, ok := dbs.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: db[%s] not configed", alias))
	}
	return eg.(*xorm.EngineGroup)
}

func logLevel(name string) core.LogLevel {
	switch name {
	case "debug":
		return core.LOG_DEBUG
	case "info":
		return core.LOG_INFO
	case "warning":
		return core.LOG_WARNING
	case "err":
		return core.LOG_ERR
	default:
		return core.LOG_OFF
	}
}

func runDBManger(ctx context.Context, opts ...*DBConfig) error {
	for _, opt := range opts {
		if opt.Enable {
			eg, err := xorm.NewEngineGroup(opt.Driver, opt.Dsn)
			if err != nil {
				log.Logf("glib: db (%s) %v", opt.Alias, err)
				continue
			}
			eg.ShowSQL(opt.ShowSql)
			eg.SetLogLevel(logLevel(opt.LogLevel))

			dbs.Store(opt.Alias, eg)
			if opt.TTL > 0 {
				go dbHealthCheck(ctx, opt.TTL, eg)
			}
		}
	}

	return nil
}

// check database health, just ping it.
func dbHealthCheck(ctx context.Context, ttl time.Duration, db *xorm.EngineGroup) {
	t := time.NewTicker(ttl * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			db.Ping()
		}
	}
}

func closeDb() {
	dbs.Range(func(key, value interface{}) bool {
		value.(*xorm.EngineGroup).Close()
		return true
	})
}
