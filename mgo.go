package glib

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
)

type mgoConfig struct {
	Enable bool          `json:"enable"`
	Alias  string        `json:"alias"`
	Dsn    string        `json:"dsn"`
	TTL    time.Duration `json:"ttl"`
}

var (
	mgos = map[string]*mgo.Session{}
)

// MgoShareCopy  will return a copy instance of `*mgo.Session`, panic if it's not exists
// example:
//	var v = make([]interface{}, 0)
//	s := glib.MgoShareCopy("something")
//	defer s.Close()
//	// use it
//	err := s.DB("somedb").C("col").Find(&v)
func MgoShareCopy(alias string) *mgo.Session {

	eg, ok := mgos[alias]
	if !ok {
		panic(fmt.Errorf("glib: mgo[%s] not configed", alias))
	}
	var v []interface{}

	eg.Copy().DB("").C("col").Find(&v)
	return eg.Copy()
}

// MgoShareClone will return a clone instance of `*mgo.Session`, panic if it's not exists
// example:
//	var v = make([]interface{}, 0)
//	s := glib.MgoShareClone("something")
//	defer s.Close()
//	// use it
//	err := s.DB("somedb").C("col").Find(&v)
func MgoShareClone(alias string) *mgo.Session {

	eg, ok := mgos[alias]
	if !ok {
		panic(fmt.Errorf("glib: mgo[%s] not configed", alias))
	}

	return eg.Clone()
}

func runMgoManager(opts ...*mgoConfig) error {
	for _, opt := range opts {
		if opt.Enable {
			s, err := mgo.DialWithTimeout(opt.Dsn, opt.TTL*time.Second)
			if err != nil {
				return fmt.Errorf("glib: mgo[%s] create err:%s", opt.Alias, err)
			}
			s.SetSyncTimeout(opt.TTL * time.Second)
			s.SetSocketTimeout(opt.TTL * time.Second)

			mgos[opt.Alias] = s
			if err = s.Ping(); err != nil {
				return fmt.Errorf("glib: mgo[%s] not health: %v", opt.Alias, err)
			}

			// do not start a goroutine to ping, mgo.v2 already do it
		}
	}

	return nil
}

func closeMgo() {
	for _, m := range mgos {
		m.Close()
	}
}
