package redis_wrapper

import "github.com/garyburd/redigo/redis"

func (w *redisWrapper) SetAdd(key string, items ...string) error {
	var args = redis.Args{key}.AddFlat(items)
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
	var args = redis.Args{key}.AddFlat(items)
	var c = w.pool.Get()
	var _, err = c.Do("SREM", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) SetDiff(key ...string) ([]string, error) {
	var args = redis.Args{}.AddFlat(key)
	var c = w.pool.Get()
	var items, err = redis.Strings(c.Do("SDIFF", args...))
	_ = c.Close()
	return items, err
}

func (w *redisWrapper) SetDiffStore(target string, key ...string) (uint, error) {
	var args = redis.Args{target}.AddFlat(key)
	var c = w.pool.Get()
	var n, err = redis.Uint64(c.Do("SDIFFSTORE", args...))
	_ = c.Close()
	return uint(n), err
}

func (w *redisWrapper) SetIntersect(key ...string) ([]string, error) {
	var args = redis.Args{}.AddFlat(key)
	var c = w.pool.Get()
	var items, err = redis.Strings(c.Do("SINTER", args...))
	_ = c.Close()
	return items, err
}

func (w *redisWrapper) SetIntersectStore(target string, key ...string) (uint, error) {
	var args = redis.Args{target}.AddFlat(key)
	var c = w.pool.Get()
	var items, err = redis.Uint64(c.Do("SINTERSTORE", args...))
	_ = c.Close()
	return uint(items), err
}

func (w *redisWrapper) SetMembers(key string) ([]string, error) {
	var c = w.pool.Get()
	var items, err = redis.Strings(c.Do("SMEMBERS", key))
	_ = c.Close()
	return items, err
}

func (w *redisWrapper) SetMoveMember(src, target, item string) (bool, error) {
	var c = w.pool.Get()
	var ok, err = redis.Bool(c.Do("SMOVE", src, target, item))
	_ = c.Close()
	return ok, err
}

func (w *redisWrapper) SetPopMember(key string, count uint) ([]string, error) {
	var ret = make([]string, count)
	if count == 0 {
		return ret, nil
	}
	var (
		v   string
		vs  []string
		err error
	)
	var c = w.pool.Get()

	if count == 1 {
		v, err = redis.String(c.Do("SPOP", key))
		if err != nil {
			ret = append(ret, v)
		}
	} else {
		vs, err = redis.Strings(c.Do("SPOP", key, count))
		if err != nil {
			ret = append(ret, vs...)
		}
	}
	_ = c.Close()
	return ret, err
}

func (w *redisWrapper) SetRandMember(key string, count uint) ([]string, error) {
	var ret = make([]string, count)
	if count == 0 {
		return ret, nil
	}
	var (
		v   string
		vs  []string
		err error
	)
	var c = w.pool.Get()

	if count == 1 {
		v, err = redis.String(c.Do("SRANDMEMBER", key))
		if err != nil {
			ret = append(ret, v)
		}
	} else {
		vs, err = redis.Strings(c.Do("SRANDMEMBER", key, count))
		if err != nil {
			ret = append(ret, vs...)
		}
	}
	_ = c.Close()
	return ret, err
}

func (w *redisWrapper) SetUnion(key ...string) ([]string, error) {
	var args = redis.Args{}.AddFlat(key)
	var c = w.pool.Get()
	var vs, err = redis.Strings(c.Do("SUNION", args...))
	_ = c.Close()
	return vs, err
}

func (w *redisWrapper) SetUnionStore(target string, key ...string) (uint, error) {
	var args = redis.Args{target}.AddFlat(key)
	var c = w.pool.Get()
	var n, err = redis.Uint64(c.Do("SUNIONSTORE", args...))
	_ = c.Close()
	return uint(n), err
}

func (w *redisWrapper) SetScan() {
	// SSCAN key cursor [MATCH pattern] [COUNT count]
}
