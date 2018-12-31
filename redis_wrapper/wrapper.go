package redis_wrapper

import (
	"io"
	"strings"
	"time"

	"github.com/carltd/glib/internal"
	"github.com/garyburd/redigo/redis"
)

const (
	redisTag = "redis"
)

type hashWrapper interface {
	// Removes the specified fields from the hash stored at key
	HashDel(key string, field ...string) error

	// Determine if a hash field exists
	HashExists(key, field string) (bool, error)

	// Get all the fields and values in a hash
	HashGet(key string, v interface{}) error

	// Increment the integer value of a hash field by the given number
	HashIncrBy(key string, field string, value int64) (int64, error)

	// Increment the float value of a hash field by the given amount
	HashIncrByFloat(key string, field string, value float64) (float64, error)

	// Get all fields in hash
	HashKeys(key string) ([]string, error)

	// Get the number of fields in a hash
	HashLen(key string) (uint, error)

	// Get the values of all given hash fields
	HashMemberGet(key string, field ...string) (map[string]interface{}, error)

	// Set multiple hash fields to multiple values
	HashMemberSet(key string, m map[string]interface{}) error

	// Set the v to hash, every field be written if the field has no zero value
	// override - false : field will not be written when the field exists
	//            true  : all field will be written
	HashSet(key string, v interface{}, override bool) error

	// Get the length of the value of a hash field
	HashStrLen(key, field string) (int, error)
}

// GeoItem for Redis Geo functions
type GeoItem struct {
	Lon, Lat, Distance float64
	Name, Hash         string
}

type geoWrapper interface {
	// Add one or more geo spatial items in the geospatial index represented
	// using a sorted set
	GeoAdd(key string, items ...*GeoItem) error

	// Returns longitude and latitude of members of a geospatial index
	GeoPosition(key string, items ...*GeoItem) error

	// Returns the distance between two members of a geospatial index
	GeoDistance(key, item1, item2 string) (float64, error)

	// Query a sorted set representing a geospatial index to fetch members
	// matching a given maximum distance from a point
	GeoRadius(key string, item *GeoItem, radius float64, limit uint) ([]*GeoItem, error)

	// Returns members of a geospatial index as standard geohash strings
	GeoHash(key string, names ...string) (map[string]string, error)
}

type setWrapper interface {
	SetAdd()
	SCARD()
	SDIFF()
	SDIFFSTORE()
	SINTER()
	SINTERSTORE()
	SISMEMBER()
	SMEMBERS()
	SMOVE()
	SPOP()
	SRANDMEMBER()
	SREM()
	SUNION()
	SUNIONSTORE()
	SSCAN()
}

type sortSetWrapper interface {
	ZADD()
	ZCARD()
	ZCOUNT()
	ZINCRBY()
	ZRANGE()
	ZRANGEBYSCORE()
	ZRANK()
	ZREM()
	ZREMRANGEBYRANK()
	ZREMRANGEBYSCORE()
	ZREVRANGE()
	ZREVRANGEBYSCORE()
	ZREVRANK()
	ZSCORE()
	ZUNIONSTORE()
	ZINTERSTORE()
	ZSCAN()
	ZRANGEBYLEX()
	ZLEXCOUNT()
	ZREMRANGEBYLEX()
}

type stringWrapper interface {
	APPEND()
	BITCOUNT()
	BITOP()
	BITFIELD()
	DECR()
	DECRBY()
	GET()
	GETBIT()
	GETRANGE()
	GETSET()
	INCR()
	INCRBY()
	INCRBYFLOAT()
	MGET()
	MSET()
	MSETNX()
	PSETEX()
	SET()
	SETBIT()
	SETEX()
	SETNX()
	SETRANGE()
	STRLEN()
}

type RedisScriptParam struct {
	Keys []interface{}
	Args []interface{}
}

type scriptWrapper interface {
	ScriptIsExists(shaValue string) (bool, error)
	ScriptEval(shaValue string, param RedisScriptParam) (interface{}, error)
}

type RedisWrapper interface {
	io.Closer
	hashWrapper
	geoWrapper
	// TODO setWrapper
	// TODO sortSetWrapper
	// TODO stringWrapper
	// TODO scriptWrapper
}

type redisWrapper struct {
	pool *redis.Pool
}

func (w *redisWrapper) Close() error {
	return w.pool.Close()
}

func Open(dsn string) (RedisWrapper, error) {
	opt, err := internal.ParseRedisDSN(dsn)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(opt.Url, "redis://") {
		opt.Url = "redis://" + opt.Url
	}

	var pool = &redis.Pool{
		MaxIdle:     opt.MaxIdle,
		MaxActive:   opt.MaxActive,
		IdleTimeout: opt.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(
				opt.Url,
				redis.DialConnectTimeout(opt.ConnectTimeout),
				redis.DialReadTimeout(opt.ReadTimeout),
				redis.DialWriteTimeout(opt.WriteTimeout),
			)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < opt.TTL {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	c := pool.Get()
	if _, err = c.Do("PING"); err != nil {
		_ = pool.Close()
		return nil, err
	}
	_ = c.Close()
	return &redisWrapper{pool: pool}, nil
}
