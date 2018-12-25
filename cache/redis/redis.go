package redis

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/carltd/glib/internal"
	"github.com/garyburd/redigo/redis"
)

type RCache struct {
	p *redis.Pool
}

func NewRedisCache(config *internal.CacheConfig) internal.Cacher {
	c := &RCache{}

	opt, err := parseRedisDSN(config.Dsn)
	if err != nil {
		panic(err)
	}

	if !strings.HasPrefix(opt.Url, "redis://") {
		opt.Url = "redis://" + opt.Url
	}

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

// Get cached Json value by key.
func (c *RCache) GetJson(key string, val interface{}) error {
	v, err := redis.String(c.Get(key))
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(v), val)
}

// Put Json value with key and expire time
func (c *RCache) PutJson(key string, val interface{}, timeout time.Duration) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return c.Put(key, string(buf), timeout)
}

func init() {
	internal.RegisterCacheDriver("redis", NewRedisCache)
}
