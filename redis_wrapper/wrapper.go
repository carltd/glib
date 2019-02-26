package redis_wrapper

import (
	"io"
	"log"
	"os"
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
	// Add one or more members to a set
	SetAdd(key string, items ...string) error

	// Get the number of members in a set
	SetLen(key string) (int, error)

	// Subtract multiple sets
	SetDiff(key ...string) ([]string, error)

	// Subtract multiple sets and store the resulting set in a key
	SetDiffStore(target string, key ...string) (uint, error)

	// Intersect multiple sets
	SetIntersect(key ...string) ([]string, error)

	// Intersect multiple sets and store the resulting set in a key
	SetIntersectStore(target string, key ...string) (uint, error)

	// Determine if a given value is a member of a set
	SetIsMember(key, item string) (bool, error)

	// Get all the members in a set
	SetMembers(key string) ([]string, error)

	// Move a member from one set to another
	// true - if the element is moved.
	// false - if the element is not a member of source and no operation was performed.
	SetMoveMember(src, target, item string) (bool, error)

	// Remove and return one or multiple random members from a set
	SetPopMember(key string, count uint) ([]string, error)

	// Get one or multiple random members from a set
	SetRandMember(key string, count uint) ([]string, error)

	// Remove one or more members from a set
	SetRemove(key string, items ...string) error

	// Add multiple sets
	SetUnion(key ...string) ([]string, error)

	// Add multiple sets and store the resulting set in a key
	SetUnionStore(target string, key ...string) (uint, error)

	// Incrementally iterate Set elements
	SetScan() // TODO: 需要实现
}

// SortSet item type
type SortSetItem struct {
	Member string
	// used by member score,
	// @note - the value must be int64 or float64 when used to zadd
	// @note - the value will be string when returned
	Score interface{}
}

type zrangeOption struct {
	limit, offset int
	scores        bool
}

type SortSetRangeOption func(opt *zrangeOption)

func WithZRange(limit, offset int) SortSetRangeOption {
	return func(o *zrangeOption) {
		o.limit = limit
		o.offset = offset
	}
}

func WithZScores() SortSetRangeOption {
	return func(o *zrangeOption) {
		o.scores = true
	}
}

type sortSetWrapper interface {
	// Add one or more members to a sorted set, or update its score if it already exists
	SortSetAdd(key string, items ...*SortSetItem) error

	// Get the number of members in a sorted set
	SortSetLen(key string) (int, error)

	// Remove one or more members from a sorted set
	SortSetRemove(key string, names ...string) error

	// Count the members in a sorted set with scores within the given values
	SortSetCount(key string, minScore, maxScore uint) (uint, error)

	// Increment the score of a member in a sorted set
	SortSetIncrBy(key string, item string, val int) (string, error)

	// Return a range of members in a sorted set, by index
	SortSetRange(key string, minIndex, maxIndex uint) ([]*SortSetItem, error)

	// Return a range of members in a sorted set, by score
	SortSetRangeByScore(key string, minScore, maxScore string, opts ...SortSetRangeOption) ([]*SortSetItem, error)

	// Determine the index of a member in a sorted set
	SortSetRank(key, name string) (uint, error)

	// Remove all members in a sorted set within the given indexes
	SortSetRemoveRangeByRank(key string, minRank, maxRank int) (uint, error)

	// Remove all members in a sorted set within the given scores
	SortSetRemoveRangeByScore(key, minScore, maxScore string) (uint, error)

	//ZREVRANGE()
	//ZREVRANGEBYSCORE()
	//ZREVRANK()

	//ZSCORE()
	//ZUNIONSTORE()
	//ZINTERSTORE()
	//ZSCAN()
	//ZRANGEBYLEX()
	//ZLEXCOUNT()
	//ZREMRANGEBYLEX()
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
	Keys []string
	Args []interface{}
}

type scriptWrapper interface {
	// Check existence of scripts in the script cache.
	ScriptIsExists(shaValue string) (bool, error)

	// Execute a Lua script server side
	ScriptEval(shaValue string, param RedisScriptParam) (interface{}, error)

	// Load the specified Lua script into the script cache.
	ScriptLoad(script string) (string, error)
}

type keyWrapper interface {
	Delete(key ...string) (int64, error)
}

type RedisWrapper interface {
	io.Closer
	hashWrapper
	geoWrapper
	setWrapper
	keyWrapper
	sortSetWrapper
	// TODO stringWrapper
	scriptWrapper

	// The returned `redis.Conn` should be closed by manual
	Raw() redis.Conn
}

type redisWrapper struct {
	pool *redis.Pool
}

func (w *redisWrapper) Raw() redis.Conn {
	return w.pool.Get()
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
			var conn, err = redis.DialURL(
				opt.Url,
				redis.DialConnectTimeout(opt.ConnectTimeout),
				redis.DialReadTimeout(opt.ReadTimeout),
				redis.DialWriteTimeout(opt.WriteTimeout),
			)
			if opt.Debug {
				conn = redis.NewLoggingConn(conn, log.New(os.Stdout, "", log.LstdFlags), "redis")
			}
			return conn, err
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
