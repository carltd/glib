package glib

import (
	"context"

	"github.com/carltd/glib/internal"
)

type featureEnabledOptions struct {
	Db     bool `json:"db"`
	Cache  bool `json:"cache"`
	Broker bool `json:"broker"`
	Mgo    bool `json:"mgo"`
	Tracer bool `json:"tracer"`
}

const (
	glibConfigEnablesKey = "glib-supports"
	glibConfigDb         = "glib-db"
	glibConfigCache      = "glib-cache"
	glibConfigMgo        = "glib-mgo"
	glibConfigTracer     = "glib-tracer"
)

var (
	ctx, stop         = context.WithCancel(context.Background())
	defEnabledOptions = &featureEnabledOptions{false, false, false, false, false}
	confCenter        *configCenter
)

// Init enabled features
func Init(opts ...option) error {

	var err error

	confCenter, err = newConfigCenter(ctx, opts...)
	if err != nil {
		return err
	}

	if err = confCenter.Load(glibConfigEnablesKey, defEnabledOptions); err != nil {
		return release(err)
	}

	// init database
	if defEnabledOptions.Db {
		dbConfig := make([]*dbConfig, 0)
		if err = confCenter.Load(glibConfigDb, &dbConfig); err != nil {
			return release(err)
		}

		if err = runDBManger(ctx, dbConfig...); err != nil {
			return release(err)
		}
	}

	// init cache
	if defEnabledOptions.Cache {
		cacheConfig := make([]*internal.CacheConfig, 0)
		if err = confCenter.Load(glibConfigCache, &cacheConfig); err != nil {
			return release(err)
		}

		if err = runCacheManger(ctx, cacheConfig...); err != nil {
			return release(err)
		}
	}

	// init mongodb
	if defEnabledOptions.Mgo {
		mgoConfig := make([]*mgoConfig, 0)
		if err = confCenter.Load(glibConfigMgo, &mgoConfig); err != nil {
			return release(err)
		}

		if err = runMgoManager(mgoConfig...); err != nil {
			return release(err)
		}
	}

	// init tracer
	if defEnabledOptions.Tracer {
		tracerAddr := confCenter.String(glibConfigTracer, defaultTracerAddr)
		if err = initTracer(tracerAddr); err != nil {
			return release(err)
		}
	}

	return nil
}

func release(err error) error {
	stop()
	closeDb()
	closeMgo()
	return closeTracer()
}

// Destroy - 释放glib管理资源
func Destroy() error {
	return release(nil)
}
