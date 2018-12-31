package glib

import (
	"context"
	"fmt"
	"sync"

	"github.com/carltd/glib/redis_wrapper"
)

type redisConfig struct {
	Enable bool   `json:"enable"`
	Alias  string `json:"alias"`
	Dsn    string `json:"dsn"`
}

var rediss sync.Map

func Redis(alias string) redis_wrapper.RedisWrapper {
	eg, ok := rediss.Load(alias)
	if !ok {
		panic(fmt.Errorf("glib: redis[%s] not configed", alias))
	}
	return eg.(redis_wrapper.RedisWrapper)
}

func runRedisManger(ctx context.Context, opts ...*redisConfig) error {
	for _, opt := range opts {
		if opt.Enable {
			c, err := redis_wrapper.Open(opt.Dsn)
			if err != nil {
				return fmt.Errorf("glib: redis[%s] create err:%v", opt.Alias, err)
			}
			rediss.Store(opt.Alias, c)
		}
	}

	return nil
}

func closeRedis() {
	dbs.Range(func(key, value interface{}) bool {
		err := value.(redis_wrapper.RedisWrapper).Close()
		return err != nil
	})
}
