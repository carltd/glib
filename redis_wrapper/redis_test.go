package redis_wrapper_test

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/carltd/glib/redis_wrapper"
	"github.com/garyburd/redigo/redis"
)

const dsn = "redis://:123456@127.0.0.1:16379/4?maxIdle=10&maxActive=10&idleTimeout=3"

type TestHashItem struct {
	IntKey   int8    `redis:"a"`
	UintKey  uint16  `redis:"b"`
	FloatKey float64 `redis:"c"`
	StrKey   string  `redis:"d"`
	BoolKey  bool    `redis:"e"`
	BytesKey []byte  `redis:"f"`
	X        string
	Y        time.Duration `redis:"y"`
	Z        time.Time     `redis:"z"`
}

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-8
}

func TestOpen(t *testing.T) {
	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Error(err)
	}
}

func TestRedisWrapper_GeoAdd(t *testing.T) {

	const key = "geo_test"

	var want = []*redis_wrapper.GeoItem{
		{Lat: 1, Lon: 2, Name: "x"},
		{Lat: 3, Lon: 4, Name: "y"},
		{Lat: 5, Lon: 6, Name: "z"},
	}

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.GeoAdd(key, want...); err != nil {
		t.Error(err)
	}

	var got = make([]*redis_wrapper.GeoItem, len(want))
	for i, item := range want {
		got[i] = &redis_wrapper.GeoItem{Name: item.Name}
	}

	if err = c.GeoPosition(key, got...); err != nil {
		t.Error(err)
	}

	for idx, item := range got {
		if item.Name != want[idx].Name {
			t.Errorf("name want %v got %v", want[idx].Name, item.Name)
		}

		if floatEqual(item.Lat, want[idx].Lat) {
			t.Errorf("lat want %v got %v", want[idx].Lat, item.Lat)
		}

		if floatEqual(item.Lon, want[idx].Lon) {
			t.Errorf("lon want %v got %v", want[idx].Lon, item.Lon)
		}
	}
}

func TestRedisWrapper_GeoHash(t *testing.T) {
	const key = "geo_test"

	var want = []*redis_wrapper.GeoItem{
		{Lat: 1, Lon: 2, Name: "x"},
		{Lat: 3, Lon: 4, Name: "y"},
		{Lat: 5, Lon: 6, Name: "z"},
	}

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.GeoAdd(key, want...); err != nil {
		t.Error(err)
	}

	result, err := c.GeoHash(key, "x", "y", "z")
	if err != nil {
		t.Error(err)
	}
	for name, v := range result {
		if v == "" {
			t.Errorf("%v got empty", name)
		}
	}
}

func TestRedisWrapper_GeoDistance(t *testing.T) {
	const key = "geo_test"

	var want = []*redis_wrapper.GeoItem{
		{Lat: 1, Lon: 2, Name: "x"},
		{Lat: 3, Lon: 4, Name: "y"},
		{Lat: 5, Lon: 6, Name: "z"},
	}

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.GeoAdd(key, want...); err != nil {
		t.Error(err)
	}

	result, err := c.GeoDistance(key, "y", "x")
	if err != nil {
		t.Error(err)
	}
	if result <= 0 {
		t.Errorf("from x to y, got %v", result)
	}
}

func TestRedisWrapper_GeoRadius(t *testing.T) {
	const key = "geo_test"

	var want = []*redis_wrapper.GeoItem{
		{Lat: 1, Lon: 2, Name: "x"},
		{Lat: 3, Lon: 4, Name: "y"},
		{Lat: 5, Lon: 6, Name: "z"},
	}

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.GeoAdd(key, want...); err != nil {
		t.Error(err)
	}

	_, err = c.GeoRadius(key, &redis_wrapper.GeoItem{Name: "x"}, 1e10, 3)
	if err != nil {
		t.Error(err)
	}

	_, err = c.GeoRadius(key, &redis_wrapper.GeoItem{Name: "not_exists"}, 1e10, 3)
	if err == nil {
		t.Error("want err got nil")
	}
}

func TestRedisWrapper_HashSet(t *testing.T) {
	const key = "hash_test_hset"
	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	var h = &TestHashItem{StrKey: "some data", UintKey: uint16(rand.Int()), BoolKey: true, Z: time.Now()}

	if err = c.HashSet(key, h, true); err != nil {
		t.Error(err)
	}

	ks, err := c.HashKeys(key)
	if err != nil {
		t.Error(err)
	}
	if len(ks) != 4 {
		t.Errorf("want 3 field got %v", len(ks))
	}

	n, err := c.HashLen(key)
	if err != nil {
		t.Error(err)
	}
	if n != 4 {
		t.Errorf("want 3 field got %v", n)
	}
}

func TestRedisWrapper_HashGet(t *testing.T) {
	const key = "hash_test_hget_all"
	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	var t1 = TestHashItem{
		IntKey:   int8(rand.Int()),
		UintKey:  uint16(rand.Uint32()),
		FloatKey: rand.Float64(),
		StrKey:   "some thing",
		BoolKey:  true,
		BytesKey: []byte{9, 8, 7, 6, 5, 4, 3, 2, 1},
		Y:        time.Second * 5,
		Z:        time.Now(),
	}

	if err = c.HashSet(key, t1, true); err != nil {
		t.Error(err)
	}

	var t2 TestHashItem
	if err = c.HashGet(key, &t2); err != nil {
		t.Error(err)
	}

	if t1.Z.Unix() != t2.Z.Unix() {
		t.Log("t1'Z not equal t2'Z")
	}
	t1.Z = t2.Z

	if !reflect.DeepEqual(t1, t2) {
		t.Logf("t1 is : %#v", t1)
		t.Logf("t2 is : %#v", t2)
		t.Error("set and get not equal")
	}

}

func TestRedisWrapper_HashStrLen(t *testing.T) {
	const key = "hash_test_hstrlen"
	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	var v1 = TestHashItem{StrKey: "xxxxx"}
	if err = c.HashSet(key, v1, true); err != nil {
		t.Error(err)
	}
	if n, err := c.HashStrLen(key, "d"); err != nil {
		t.Fatal(err)
	} else {
		if n != len(v1.StrKey) {
			t.Errorf("hstrlen want %d got %d", len(v1.StrKey), n)
		}
	}
}

func TestRedisWrapper_HashIncrBy(t *testing.T) {
	const (
		key    = "hash_test_hincrby"
		subKey = "count"
	)

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.HashDel(key, subKey); err != nil {
		t.Fatal(err)
	}

	n, err := c.HashIncrBy(key, subKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("HashIncrBy want %d got %d", 1, n)
	}
}

func TestRedisWrapper_HashIncrByFloat(t *testing.T) {
	const (
		key    = "hash_test_hincrbyfloat"
		subKey = "count"
	)

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.HashDel(key, subKey); err != nil {
		t.Fatal(err)
	}

	n, err := c.HashIncrByFloat(key, subKey, 1.1)
	if err != nil {
		t.Fatal(err)
	}
	if !floatEqual(n, 1.1) {
		t.Errorf("HashIncrByFloat want %f got %f", 1.1, n)
	}

	if _, err = c.Delete(key); err != nil {
		t.Fatal(err)
	}
}

func TestRedisWrapper_HashMemberSet_and_HashMemberGet(t *testing.T) {
	var (
		key   = "hash_test_member"
		m1, m map[string]interface{}
		keys  = []string{"k1", "k2", "k3", "k4"}
		k1    int
		k2    float64
		k3    bool
		k4    string
	)
	m = map[string]interface{}{
		"k1": 1,
		"k2": 3.14,
		"k3": false,
		"k4": "some data",
	}

	var c, err = redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if err = c.HashMemberSet(key, m); err != nil {
		t.Fatal(err)
	}

	if m1, err = c.HashMemberGet(key, keys...); err != nil {
		t.Fatal(err)
	}

	for k, v := range m {
		switch k {
		case "k1":
			k1, err = redis.Int(m1[k], nil)
			if err != nil {
				t.Errorf(" k1 type is %T, got err %v", v, err)
			}
			if k1 != v {
				t.Errorf(" k1 want %v, got %v", v, k1)
			}
		case "k2":
			k2, err = redis.Float64(m1[k], nil)
			if err != nil {
				t.Errorf(" k2 type is %T, got err %v", v, err)
			}
			if k2 != v {
				t.Errorf(" k2 want %v, got %v", v, k2)
			}
		case "k3":
			k3, err = redis.Bool(m1[k], nil)
			if err != nil {
				t.Errorf(" k3 type is %T, got err %v", v, err)
			}
			if k3 != v {
				t.Errorf(" k3 want %v, got %v", v, k3)
			}
		case "k4":
			k4, err = redis.String(m1[k], nil)
			if err != nil {
				t.Errorf(" k4 type is %T, got err %v", v, err)
			}
			if k4 != v {
				t.Errorf(" k4 want %v, got %v", v, k4)
			}
		}
	}
}

func TestRedisWrapper_Set(t *testing.T) {
	var (
		n      int
		exists bool
		key    = "set_test_add"
		items  = []string{"item1", "item2", "item3"}
	)

	c, err := redis_wrapper.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if _, err = c.Delete(key); err != nil {
		t.Fatal(err)
	}

	if err = c.SetAdd(key, items...); err != nil {
		t.Fatal(err)
	}

	if n, err = c.SetLen(key); err != nil {
		t.Fatal(err)
	}
	if n != len(items) {
		t.Errorf("scard want %d, got %d", len(items), n)
	}

	if err = c.SetRemove(key, items[0]); err != nil {
		t.Fatal(err)
	}

	if exists, err = c.SetIsMember(key, items[0]); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("sismember want false, got true")
	}

	if _, err = c.Delete(key); err != nil {
		t.Fatal(err)
	}
}
