package glib

import (
	"context"
	"fmt"
	"sync"

	"github.com/carltd/glib/v2/internal"
)

var (
	caches sync.Map
)

func Cache(alias string) internal.Cacher {
	c, ok := caches.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: cache[%s] not configed", alias))
	}
	return c.(internal.Cacher)
}

func runCacheManger(ctx context.Context, opts ...*internal.CacheConfig) error {

	for _, opt := range opts {
		if opt.Enable {
			cacheCreator, ok := internal.CacheDriver(opt.Driver)
			if ok {
				caches.Store(opt.Alias, cacheCreator(opt))
			} else {
				panic(fmt.Errorf("glib: cache[%s] init err", opt.Alias))
			}
		}
	}
	return nil
}
