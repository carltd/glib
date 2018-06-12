package glib

import (
	"context"

)

type featureEnabledOptions struct {
	Db      bool `json:"db"`
	Cache   bool `json:"cache"`
	Queue   bool `json:"queue"`
	Mongodb bool `json:"mongodb"`
}

const (
	kGlibConfigEnablesKey = "glib-supports"
	kGlibConfigDb         = "glib-db"
	kGlibConfigCache      = "glib-cache"
)

var (
	ctx, stop         = context.WithCancel(context.Background())
	defEnabledOptions = &featureEnabledOptions{false, false, false, false}
	confCenter        *configCenter
)

// Init enabled features
func Init(opts ...Option) error {

	var err error

	confCenter, err = newConfigCenter(ctx, opts...)
	if err != nil {
		return err
	}

	//log.Logf("config center loaded: %s", confCenter.conf.Bytes())s

	if err = confCenter.Load(kGlibConfigEnablesKey, defEnabledOptions); err != nil {
		return release(err)
	}

	// init database
	if defEnabledOptions.Db {
		dbConfig := make([]*DBConfig, 0)
		if err = confCenter.Load(kGlibConfigDb, &dbConfig); err != nil {
			return release(err)
		}

		if err = runDBManger(ctx, dbConfig...); err != nil {
			return release(err)
		}
	}

	// init cache
	if defEnabledOptions.Cache {
		cacheConfig := make([]*CacheConfig, 0)
		if err = confCenter.Load(kGlibConfigCache, &cacheConfig); err != nil {
			return release(err)
		}

		if err = runCacheManger(ctx, cacheConfig...); err != nil {
			return release(err)
		}
	}

	return nil
}

func release(err error) error {
	stop()
	closeDb()
	return err
}

func Destroy() error {
	return release(nil)
}
