package glib

import (
	"context"
	"github.com/carltd/glib/internal"
)

type featureEnabledOptions struct {
	Db    bool `json:"db"`
	Cache bool `json:"cache"`
	Queue bool `json:"queue"`
	Mgo   bool `json:"mgo"`
}

const (
	kGlibConfigEnablesKey = "glib-supports"
	kGlibConfigDb         = "glib-db"
	kGlibConfigCache      = "glib-cache"
	kGlibConfigMgo        = "glib-mgo"
)

var (
	ctx, stop         = context.WithCancel(context.Background())
	defEnabledOptions = &featureEnabledOptions{false, false, false, false}
	confCenter        *configCenter
)

// Init enabled features
func Init(opts ...option) error {

	var err error

	confCenter, err = newConfigCenter(ctx, opts...)
	if err != nil {
		return err
	}

	if err = confCenter.Load(kGlibConfigEnablesKey, defEnabledOptions); err != nil {
		return release(err)
	}

	// init database
	if defEnabledOptions.Db {
		dbConfig := make([]*dbConfig, 0)
		if err = confCenter.Load(kGlibConfigDb, &dbConfig); err != nil {
			return release(err)
		}

		if err = runDBManger(ctx, dbConfig...); err != nil {
			return release(err)
		}
	}

	// init cache
	if defEnabledOptions.Cache {
		cacheConfig := make([]*internal.CacheConfig, 0)
		if err = confCenter.Load(kGlibConfigCache, &cacheConfig); err != nil {
			return release(err)
		}

		if err = runCacheManger(ctx, cacheConfig...); err != nil {
			return release(err)
		}
	}

	// init mongodb
	if defEnabledOptions.Mgo {
		mgoConfig := make([]*mgoConfig, 0)
		if err = confCenter.Load(kGlibConfigMgo, &mgoConfig); err != nil {
			return release(err)
		}

		if err = runMgoManager(mgoConfig...); err != nil {
			return release(err)
		}
	}

	return nil
}

func release(err error) error {
	stop()
	closeDb()
	closeMgo()
	return err
}

func Destroy() error {
	return release(nil)
}
