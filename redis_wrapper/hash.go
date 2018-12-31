package redis_wrapper

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

func (w *redisWrapper) HashDel(key string, field ...string) error {
	var (
		args = redis.Args{key}
		err  error
	)
	args = args.AddFlat(field)

	var c = w.pool.Get()
	_, err = c.Do("HDEL", args...)
	_ = c.Close()
	return err
}

func (w *redisWrapper) HashExists(key, field string) (bool, error) {
	var c = w.pool.Get()
	var exists, err = redis.Bool(c.Do("HEXISTS", key, field))
	_ = c.Close()
	return exists, err
}

// Set the v to hash, every field be written if the field has no zero value
// override - false : field will not be written when the field exists
//            true  : all field will be written
func (w *redisWrapper) HashSet(key string, v interface{}, override bool) error {
	var (
		args         = redis.Args{key}
		valV         = reflect.ValueOf(v)
		typV         = valV.Type()
		redisHashKey string
		err          error
	)

	switch typV.Kind() {
	case reflect.Ptr:
		if typV.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("HashSet %T not a struct", v)
		}
		typV = typV.Elem()
		valV = valV.Elem()
	case reflect.Struct:
	default:
		return fmt.Errorf("HashSet %T not a struct", v)
	}

	for i := 0; i < typV.NumField(); i++ {
		redisHashKey = typV.Field(i).Tag.Get(redisTag)
		if redisHashKey == "" {
			continue
		}

		switch valV.Field(i).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if valV.Field(i).Int() != 0 {
				args = args.Add(redisHashKey, valV.Field(i).Int())
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if valV.Field(i).Uint() > 0 {
				args = args.Add(redisHashKey, valV.Field(i).Uint())
			}
		case reflect.Float32, reflect.Float64:
			if valV.Field(i).Float() > 0 {
				args = args.Add(redisHashKey, valV.Field(i).Float())
			}
		case reflect.String:
			if valV.Field(i).String() != "" {
				args = args.Add(redisHashKey, valV.Field(i).String())
			}
		case reflect.Bool:
			args = args.Add(redisHashKey, valV.Field(i).Bool())
		case reflect.Slice:
			if len(valV.Field(i).Bytes()) > 0 {
				args = args.Add(redisHashKey, valV.Field(i).Bytes())
			}
		}
	}

	var c = w.pool.Get()
	if override {
		_, err = c.Do("HMSET", args...)
	} else {
		_, err = c.Do("HSETNX", args...)
	}
	_ = c.Close()
	return err
}

// Get all fields in hash
func (w *redisWrapper) HashKeys(key string) ([]string, error) {
	var c = w.pool.Get()
	var fields, err = redis.Strings(c.Do("HKEYS", key))
	_ = c.Close()
	return fields, err
}

// Get the number of fields in a hash
func (w *redisWrapper) HashLen(key string) (uint, error) {
	var c = w.pool.Get()
	var n, err = redis.Int(c.Do("HLEN", key))
	_ = c.Close()
	return uint(n), err
}

func (w *redisWrapper) HashGet(key string, v interface{}) error {
	var (
		args         = redis.Args{key}
		valV         = reflect.ValueOf(v)
		typV         = valV.Type()
		redisHashKey string
		err          error
		tmp          []string
		val          string
		ok           bool
		f            float64
		n            int64
		un           uint64
		ks           []string
	)

	if typV.Kind() != reflect.Ptr || typV.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("HashGet %T not a struct", v)
	}

	typV = typV.Elem()
	valV = valV.Elem()

	for i := 0; i < typV.NumField(); i++ {
		redisHashKey = typV.Field(i).Tag.Get(redisTag)
		if redisHashKey == "" {
			continue
		}
		ks = append(ks, redisHashKey)
	}

	var c = w.pool.Get()
	tmp, err = redis.Strings(c.Do("HMGET", args.AddFlat(ks)...))
	_ = c.Close()

	if err != nil {
		return err
	}

	tmpMap := make(map[string]string)
	for i := 0; i < len(ks); i++ {
		tmpMap[ks[i]] = tmp[i]
	}

	for i := 0; i < typV.NumField(); i++ {
		redisHashKey = typV.Field(i).Tag.Get(redisTag)
		if redisHashKey == "" {
			continue
		}
		if val, ok = tmpMap[redisHashKey]; ok {
			switch valV.Field(i).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				n, _ = strconv.ParseInt(val, 10, 0)
				valV.Field(i).SetInt(n)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				un, _ = strconv.ParseUint(val, 10, 0)
				valV.Field(i).SetUint(un)
			case reflect.Float32, reflect.Float64:
				f, _ = strconv.ParseFloat(val, 0)
				valV.Field(i).SetFloat(f)
			case reflect.String:
				valV.Field(i).SetString(val)
			case reflect.Slice:
				valV.Field(i).SetBytes([]byte(val))
			case reflect.Bool:
				ok, _ = strconv.ParseBool(val)
				valV.Field(i).SetBool(ok)
			}
		}
	}

	return nil
}

// Increment the integer value of a hash field by the given number
func (w *redisWrapper) HashIncrBy(key string, field string, value int64) (int64, error) {
	var c = w.pool.Get()
	var n, err = redis.Int64(c.Do("HINCRBY", key, field, value))
	_ = c.Close()
	return n, err
}

// Increment the float value of a hash field by the given amount
func (w *redisWrapper) HashIncrByFloat(key string, field string, value float64) (float64, error) {
	var c = w.pool.Get()
	var n, err = redis.Float64(c.Do("HINCRBYFLOAT", key, field, value))
	_ = c.Close()
	return n, err
}

// Get the values of all given hash fields
func (w *redisWrapper) HashMemberGet(key string, field ...string) (map[string]interface{}, error) {
	var args = redis.Args{key}

	var c = w.pool.Get()
	var valArr, err = redis.Values(c.Do("HMGET", args.AddFlat(field)...))
	_ = c.Close()
	if err != nil {
		return nil, err
	}

	var ret = make(map[string]interface{})
	for i := 0; i < len(field); i++ {
		ret[field[i]] = valArr[i]
	}
	return ret, nil
}

// Set multiple hash fields to multiple values
func (w *redisWrapper) HashMemberSet(key string, m map[string]interface{}) error {
	var (
		args = redis.Args{key}
		err  error
	)

	for k, v := range m {
		args = args.Add(k, v)
	}

	var c = w.pool.Get()
	_, err = c.Do("HMSET", args...)
	_ = c.Close()
	return err
}

// Get the length of the value of a hash field
func (w *redisWrapper) HashStrLen(key, field string) (int, error) {
	var c = w.pool.Get()
	var n, err = redis.Int(c.Do("HSTRLEN", key, field))
	_ = c.Close()
	return n, err
}
