package redis_wrapper

import "github.com/garyburd/redigo/redis"

func (w *redisWrapper) ScriptIsExists(shaValue string) (bool, error) {
	var c = w.pool.Get()
	var exists, err = redis.Bool(c.Do("SCRIPT", "EXISTS", shaValue))
	_ = c.Close()
	return exists, err
}

func (w *redisWrapper) ScriptLoad(script string) (string, error) {
	var c = w.pool.Get()
	var shaValue, err = redis.String(c.Do("SCRIPT", "LOAD", script))
	_ = c.Close()
	return shaValue, err
}

func (w *redisWrapper) ScriptEval(shaValue string, param RedisScriptParam) (interface{}, error) {
	var args = redis.Args{shaValue}
	args = args.Add(len(param.Keys))
	args = args.AddFlat(param.Keys)
	args = args.AddFlat(param.Args)

	var c = w.pool.Get()
	var result, err = c.Do("EVALSHA", args...)
	_ = c.Close()
	return result, err
}
