package redis_wrapper

import "github.com/garyburd/redigo/redis"

func (w *redisWrapper) Delete(keys ...string) (int64, error) {
	var args = redis.Args([]interface{}{})
	args = args.AddFlat(keys)

	var c = w.pool.Get()
	var n, err = redis.Int64(c.Do("DEL", args...))
	_ = c.Close()
	return n, err
}
