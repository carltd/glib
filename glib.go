package glib

import (
	"context"
	"github.com/carltd/glib/v2/internal"
	gtrace "github.com/carltd/glib/v2/trace"
)

type featureEnabledOptions struct {
	Db     bool `json:"db"`
	Cache  bool `json:"cache"`
	Broker bool `json:"broker"`
	Mgo    bool `json:"mgo"`
	Tracer bool `json:"tracer"`
	Redis  bool `json:"redis"`
}

// All keys only placed in the root directory
const (
	glibConfigEnablesKey = "glib-supports"
	glibConfigDb         = "glib-db"
	glibConfigCache      = "glib-cache"
	glibConfigMgo        = "glib-mgo"
	glibConfigTracer     = "glib-tracer"
	glibConfigBroker     = "glib-broker"
	glibConfigRedis      = "glib-redis"
)

var (
	ctx, stop         = context.WithCancel(context.Background())
	defEnabledOptions = &featureEnabledOptions{false, false, false, false, false, false}
	confCenter        *configCenter
)

// Init enabled features
func Init(opts ...option) error {

	var err error

	confCenter, err = newConfigCenter(opts...)
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

	// init broker
	if defEnabledOptions.Broker {
		brokerConfig := make([]*brokerConfig, 0)
		if err = confCenter.Load(glibConfigBroker, &brokerConfig); err != nil {
			return release(err)
		}
		if err = runBrokerManager(brokerConfig...); err != nil {
			return release(err)
		}
	}

	if defEnabledOptions.Redis {
		rdsConfig := make([]*redisConfig, 0)
		if err = confCenter.Load(glibConfigRedis, &rdsConfig); err != nil {
			return release(err)
		}
		if err = runRedisManger(ctx, rdsConfig...); err != nil {
			return release(err)
		}
	}

	// init tracer
	if defEnabledOptions.Tracer {
		tCfg := gtrace.TracerConfig{}
		if err = confCenter.Load(glibConfigTracer, &tCfg); err != nil {
			return release(err)
		}
		tCfg.SrvName = confCenter.Options().ServiceDomain
		tCfg.HostPort = confCenter.Options().RunAt
		if err = gtrace.InitTracer(tCfg); err != nil {
			return release(err)
		}
	}

	return nil
}

// get custom config Object
func GetConfig(keyPath string) ConfigObject {
	return confCenter.Raw(keyPath)
}

func release(err error) error {
	stop()
	closeDb()
	closeMgo()
	closeBroker()
	closeRedis()
	return err
}

// Destroy - release all resource
func Destroy() error {
	return release(nil)
}
