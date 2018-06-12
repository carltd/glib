package glib

import (
	"sync"
	"context"
)

var (
	cacheDrivers sync.Map
	caches       sync.Map
)


func Cache(alias string) Cacher {
	c, ok := caches.Load(alias)
	if !ok {
		//panic(fmt.Errorf("glib: cache[%s] not configed", alias))
	}
	return c.(Cacher)
}

func RegisterCacheDriver(driverName string, creator CacheCreator) {
	cacheDrivers.Store(driverName, creator)
}


func runCacheManger(ctx context.Context, opts ...*CacheConfig) error {

	for _, opt := range opts {
		if opt.Enable {
			cacheCreator, ok := cacheDrivers.Load(opt.Driver)
			if  ok {
				caches.Store(opt.Alias, cacheCreator.(CacheCreator)(opt))
			}
		}
	}
	return nil
}
