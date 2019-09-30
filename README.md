# glib

## Install glib
```bash
#install glib
go get github.com/carltd/glib/v2
dep ensure
```

## Usage

start `consul` as config center
```bash
consul agent -ui -server -data-dir=/tmp

vi ~/glib-test.go
```
intput content:
```go
package main

import (
	"time"

	"github.com/micro/go-log"

	"github.com/carltd/glib/v2"
	_ "github.com/carltd/glib/v2/cache/memcache"
	_ "github.com/carltd/glib/v2/cache/redis"
	"gopkg.in/mgo.v2/bson"
)

type User struct {
	Id   uint   `xorm:"id"`
	Name string `xorm:"name"`
}

const (
	cacheKey    = "test-k"
	cacheValue  = "1"
	cacheExpire = time.Hour
)

func main() {
	var (
		user   User
		err    error
		exists bool
	)

	// init glib with config center(consul)
	if err = glib.Init(glib.WithServiceDomain("com.carltd.srv.demo")); err != nil {
		log.Fatal(err)
	}
	defer glib.Destroy()

	// cache usage
	log.Log(glib.Cache("rc").Put(cacheKey, cacheValue, cacheExpire))
	log.Log(glib.Cache("mc").Put(cacheKey, cacheValue, cacheExpire))

	log.Log(glib.Cache("rc").Get(cacheKey))
	log.Log(glib.Cache("mc").Get(cacheKey))

	// mysql usage
	exists, err = glib.DB("db1").Get(&user)
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		log.Logf("%+v", user)
	}

	// mongodb usage
	var v interface{}
	s := glib.MgoShareClone("mgo")
	err = s.DB("test").C("col1").Find(bson.M{}).One(&v)
	s.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Logf("%+v", v)


	s = glib.MgoShareCopy("mgo")
	err = s.DB("test").C("col1").Find(bson.M{}).One(&v)
	s.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Logf("%+v", v)
}

```

config consul kv
**\com.carltd.srv.demo\glib-supports**:
```json
{
    "db": true,
    "cache": true,
    "mgo": true
}
```

**\com.carltd.srv.demo\glib-db**:
```json
[{
    "alias": "db1",
    "driver": "mysql",
    "showSql": false,
    "logLevel": "debug",
    "dsn": "root:@tcp(127.0.0.1:3306)/test",
    "enable": true,
    "ttl": 30
}]
```

**\com.carltd.srv.demo\glib-cache**:
```json
[{
    "alias": "rc",
    "driver": "redis",
    "dsn": ":123456@127.0.0.1:6379/0",
    "enable": true,
    "ttl": 30
},{
    "alias":"mc",
    "driver":"memcache",
    "dsn": "127.0.0.1:11211",
    "enable": true,
    "ttl": 30
}]
```

**\com.carltd.srv.demo\glib-mgo**:
```json
[{
    "enable": true,
    "alias": "mgo",
    "ttl":60,
    "dsn": "mongodb://127.0.0.1:27017"
}]
```


at last, run glib-test.go
```
go run glib-test.go
```



