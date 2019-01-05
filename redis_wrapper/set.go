package redis_wrapper

import "github.com/garyburd/redigo/redis"

func (w *redisWrapper) SetAdd(key string, items ...string) error {
	var args = redis.Args([]interface{}{}).Add(key)
	args = args.AddFlat(items)

	var c = w.pool.Get()
	var _, err = c.Do("SADD", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) SetLen(key string) (int, error) {
	var c = w.pool.Get()
	var n, err = redis.Int(c.Do("SCARD", key))
	_ = c.Close()
	return n, err
}

func (w *redisWrapper) SetIsMember(key, item string) (bool, error) {
	var c = w.pool.Get()
	var exists, err = redis.Bool(c.Do("SISMEMBER", key, item))
	_ = c.Close()
	return exists, err
}

func (w *redisWrapper) SetRemove(key string, items ...string) error {
	var args = redis.Args([]interface{}{}).Add(key)
	args = args.AddFlat(items)

	var c = w.pool.Get()
	var _, err = c.Do("SREM", args...)
	_ = c.Close()
	return err
}
