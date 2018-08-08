package glib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/micro/go-log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

// DBConfig is config for struct
type dbConfig struct {
	Enable bool          `json:"enable"`
	Debug  bool          `json:"debug"`
	Alias  string        `json:"alias"`
	Driver string        `json:"driver"`
	Dsn    string        `json:"dsn"`
	TTL    time.Duration `json:"ttl"`
}

var dbs sync.Map

// DB will return a instance of `xorm.EngineGroup`, panic if it's not exists
func DB(alias string) *gorm.DB {
	eg, ok := dbs.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: db[%s] not configed", alias))
	}
	return eg.(*gorm.DB)
}

func runDBManger(ctx context.Context, opts ...*dbConfig) error {
	for _, opt := range opts {
		if opt.Enable {
			db, err := gorm.Open(opt.Driver, opt.Dsn)
			if err != nil {
				log.Logf("glib: db (%s) %v", opt.Alias, err)
				continue
			}
			// TODO: transport params
			//db.DB().SetMaxIdleConns(10)
			//db.DB().SetMaxOpenConns(10)
			db.LogMode(opt.Debug)
			db.SingularTable(true)
			dbs.Store(opt.Alias, db)
			if opt.TTL > 0 {
				go dbHealthCheck(ctx, opt.TTL, db)
			}
		}
	}

	return nil
}

// check database health, just ping it.
func dbHealthCheck(ctx context.Context, ttl time.Duration, db *gorm.DB) {
	t := time.NewTicker(ttl * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			db.DB().Ping()
		}
	}
}

func closeDb() {
	dbs.Range(func(key, value interface{}) bool {
		err := value.(*gorm.DB).Close()
		return err != nil
	})
}
