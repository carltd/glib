package redis_wrapper

import (
	"github.com/garyburd/redigo/redis"
)

func (w *redisWrapper) SortSetAdd(key string, items ...*SortSetItem) error {
	if len(items) == 0 {
		return nil
	}

	var args = redis.Args{key}
	for _, item := range items {
		args = args.Add(item.Score, item.Member)
	}

	var c = w.pool.Get()
	var _, err = c.Do("ZADD", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) SortSetLen(key string) (int, error) {
	var c = w.pool.Get()
	var n, err = redis.Int(c.Do("ZCARD", key))
	_ = c.Close()
	return n, err
}

func (w *redisWrapper) SortSetRemove(key string, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	var args = redis.Args{key}.AddFlat(names)
	var c = w.pool.Get()
	var _, err = c.Do("ZREM", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) SortSetCount(key string, minScore, maxScore uint) (uint, error) {
	var c = w.pool.Get()
	var n, err = redis.Uint64(c.Do("ZCOUNT", key, minScore, maxScore))
	_ = c.Close()
	return uint(n), err
}

func (w *redisWrapper) SortSetIncrBy(key string, item string, val int) (string, error) {
	var c = w.pool.Get()
	var score, err = redis.String(c.Do("ZINCRBY", key, val, item))
	_ = c.Close()
	return score, err
}

func (w *redisWrapper) SortSetRange(key string, minIndex, maxIndex uint) ([]*SortSetItem, error) {
	var ret []*SortSetItem
	var c = w.pool.Get()
	var items, err = redis.Strings(c.Do("ZRANGE", key, minIndex, maxIndex, "WITHSCORES"))
	_ = c.Close()
	if err != nil {
		ret = make([]*SortSetItem, 0)
		for i := 0; i < len(items); i += 2 {
			ret = append(ret, &SortSetItem{Member: items[i], Score: items[i+1]})
		}
	}
	return ret, err
}

func (w *redisWrapper) SortSetRangeByScore(key string, minScore, maxScore string, opts ...SortSetRangeOption) ([]*SortSetItem, error) {
	var (
		option *zrangeOption
		ret    []*SortSetItem
		err    error
		items  []string
		args   = redis.Args{key}.Add(minScore, maxScore)
		i      int
	)

	if len(opts) > 0 {
		option = new(zrangeOption)
		for _, o := range opts {
			o(option)
		}
	}

	if option != nil {
		if option.scores {
			args = args.Add("WITHSCORES")
		}

		args = args.Add("LIMIT", option.offset, option.limit)
	}

	var c = w.pool.Get()
	items, err = redis.Strings(c.Do("ZRANGEBYSCORE", args...))
	_ = c.Close()

	if err != nil {
		ret = make([]*SortSetItem, 0)
		if option.scores {
			for i = 0; i < len(items); i += 2 {
				ret = append(ret, &SortSetItem{Member: items[i], Score: items[i+1]})
			}
		} else {
			for i = 0; i < len(items); i++ {
				ret = append(ret, &SortSetItem{Member: items[i]})
			}
		}

	}
	return ret, err
}

//TODO: no tested
func (w *redisWrapper) SortSetRank(key, name string) (uint, error) {
	var c = w.pool.Get()
	var rank, err = redis.Uint64(c.Do("ZRANK", key, name))
	_ = c.Close()
	return uint(rank), err
}

func (w *redisWrapper) SortSetRemoveRangeByRank(key string, minRank, maxRank int) (uint, error) {
	var c = w.pool.Get()
	var rank, err = redis.Uint64(c.Do("ZREMRANGEBYRANK", key, minRank, maxRank))
	_ = c.Close()
	return uint(rank), err
}

func (w *redisWrapper) SortSetRemoveRangeByScore(key, minScore, maxScore string) (uint, error) {
	var c = w.pool.Get()
	var rank, err = redis.Uint64(c.Do("ZREMRANGEBYSCORE", key, minScore, maxScore))
	_ = c.Close()
	return uint(rank), err
}
