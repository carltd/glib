package redis_wrapper

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

func (w *redisWrapper) GeoAdd(key string, items ...*GeoItem) error {
	var args = redis.Args([]interface{}{}).Add(key)
	for _, item := range items {
		args = args.Add(item.Lon, item.Lat, item.Name)
	}

	var c = w.pool.Get()
	var _, err = redis.Int(c.Do("GEOADD", args...))
	_ = c.Close()
	return err
}

func (w *redisWrapper) GeoPosition(key string, items ...*GeoItem) error {
	var args = redis.Args([]interface{}{}).Add(key)
	for _, item := range items {
		args = args.Add(item.Name)
	}

	var c = w.pool.Get()
	var arrSlice, err = redis.Positions(c.Do("GEOPOS", args...))
	_ = c.Close()
	if err != nil {
		return err
	}

	for idx, v := range arrSlice {
		if v != nil {
			items[idx].Lon = v[0]
			items[idx].Lat = v[1]
		}
	}

	return err
}

func (w *redisWrapper) GeoDistance(key, item1, item2 string) (float64, error) {
	var c = w.pool.Get()
	var dis, err = redis.Float64(c.Do("GEODIST", key, item1, item2, "m"))
	_ = c.Close()
	return dis, err
}

func parsePosition(v interface{}) (float64, float64, error) {
	val, err := redis.Float64s(v, nil)
	return val[0], val[1], err
}

func floatFmt(f float64) string {
	return fmt.Sprintf("%.6f", f)
}

func (w *redisWrapper) GeoRadius(key string, item *GeoItem, radius float64, limit uint) ([]*GeoItem, error) {
	var (
		err    error
		result []interface{}
		c      = w.pool.Get()
		ret    []*GeoItem
	)
	if len(item.Name) > 0 {
		result, err = redis.Values(
			c.Do("GEORADIUSBYMEMBER",
				key, item.Name, floatFmt(radius), "m",
				"WITHCOORD", "COUNT", limit),
		)
	} else {
		result, err = redis.Values(
			c.Do("GEORADIUS",
				key, floatFmt(item.Lon), floatFmt(item.Lat), radius, "m",
				"WITHCOORD", "COUNT", limit),
		)
	}
	_ = c.Close()
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		ret = make([]*GeoItem, len(result))
		for idx, v := range result {
			vv := v.([]interface{})
			lon, lat, err := parsePosition(vv[1])
			if err != nil {
				return nil, err
			}
			ret[idx] = &GeoItem{
				Name: string(vv[0].([]uint8)),
				Lat:  lat,
				Lon:  lon,
			}
		}
	}

	return ret, nil
}

func (w *redisWrapper) GeoHash(key string, names ...string) (map[string]string, error) {
	var args = redis.Args([]interface{}{}).Add(key)
	args = args.AddFlat(names)

	var c = w.pool.Get()
	var geohashes, err = redis.Strings(c.Do("GEOHASH", args...))
	_ = c.Close()
	if err != nil {
		return nil, err
	}

	var ret = make(map[string]string, len(names))
	for i, name := range names {
		ret[name] = geohashes[i]
	}
	return ret, nil
}
