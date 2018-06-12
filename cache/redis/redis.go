package redis

import (
	"time"

	"github.com/carltd/glib"
	"github.com/garyburd/redigo/redis"
	"github.com/micro/go-log"
)

type RCache struct {
	p *redis.Pool
}

func NewRedisCache(config *glib.CacheConfig) glib.Cacher {
	c := &RCache{}

	opt, err := parseRedisDSN(config.Dsn)
	if err != nil {
		log.Fatal(err)
	}

	opt.Url = "redis://" + opt.Url

	c.p = &redis.Pool{
		MaxIdle:     opt.MaxIdle,
		MaxActive:   opt.MaxActive,
		IdleTimeout: opt.IdleTimeout * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(
				opt.Url,
				redis.DialConnectTimeout(opt.ConnectTimeout),
				redis.DialReadTimeout(opt.ReadTimeout),
				redis.DialWriteTimeout(opt.WriteTimeout),
			)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < config.TTL*time.Second {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return c
}

func (c *RCache) Get(key string) (result interface{}, err error) {
	conn := c.p.Get()
	result, err = conn.Do("GET", key)
	conn.Close()
	return result, err
}

func (c *RCache) Put(key string, val interface{}, timeout time.Duration) (err error) {
	conn := c.p.Get()
	if timeout >= time.Second {
		_, err = conn.Do("SETEX", key, int(timeout/time.Second), val)
	} else {
		_, err = conn.Do("SET", key, val)
	}
	conn.Close()
	return err
}

func (c *RCache) Delete(key string) (err error) {
	conn := c.p.Get()
	_, err = conn.Do("DEL", key)
	conn.Close()
	return err
}

// Incr increase counter in redis.
func (c *RCache) Increment(key string) (err error) {
	conn := c.p.Get()
	_, err = conn.Do("INCRBY", key, 1)
	conn.Close()
	return err
}

// Decr decrease counter in redis.
func (c *RCache) Decrement(key string) (err error) {
	conn := c.p.Get()
	_, err = conn.Do("INCRBY", key, -1)
	conn.Close()
	return err
}

func (c *RCache) Touch(key string, timeout time.Duration) (err error) {
	conn := c.p.Get()
	_, err = conn.Do("EXPIRE", key, int(timeout/time.Second))
	conn.Close()
	return err
}

// FlushAll clear all cached in redis
func (c *RCache) ClearAll() (err error) {
	conn := c.p.Get()
	_, err = conn.Do("FLUSHDB")
	conn.Close()
	return err
}

func init() {
	glib.RegisterCacheDriver("redis", NewRedisCache)
}
