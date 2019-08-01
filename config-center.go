package glib

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hashicorp/consul/api"
)

type ConfigObject interface {
	StringSlice(def []string) []string
	StringMap(def map[string]string) map[string]string
	Scan(val interface{}) error
	Bytes() []byte
}

type configObject []byte

func (o configObject) StringSlice(def []string) []string {
	var s = make([]string, 0)
	if json.Unmarshal(o, &s) == nil {
		return s
	}
	return def
}
func (o configObject) StringMap(def map[string]string) map[string]string {
	var m = make(map[string]string)
	if json.Unmarshal(o, &m) == nil {
		return m
	}
	return def
}
func (o configObject) Scan(val interface{}) error { return json.Unmarshal(o, val) }
func (o configObject) Bytes() []byte              { return o }

type configCenter struct {
	opts   options
	rawMap map[string][]byte
}

func newConfigCenter(opts ...option) (*configCenter, error) {
	cc := &configCenter{
		rawMap: make(map[string][]byte),
	}
	err := cc.Init(opts...)
	return cc, err
}

func (cc *configCenter) Init(opts ...option) error {

	cc.opts = newOptions(opts...)

	cfg := api.DefaultConfig()
	cfg.Address = cc.opts.DiscoverAddr
	client, err := api.NewClient(cfg)
	if err != nil {
		return err
	}

	kv, _, err := client.KV().List(cc.opts.ServiceDomain, nil)
	if err != nil {
		return err
	}

	if kv != nil && len(kv) > 0 {
		for _, v := range kv {
			k := strings.TrimPrefix(strings.TrimPrefix(v.Key, cc.opts.ServiceDomain), "/")
			cc.rawMap[k] = v.Value
		}
	}

	return nil
}

func (cc *configCenter) String(key, defValue string) string {
	if val, ok := cc.rawMap[key]; ok {
		return string(val)
	}
	return defValue
}

func (cc *configCenter) Load(key string, v interface{}) error {
	if val, ok := cc.rawMap[key]; ok {
		return json.Unmarshal(val, v)
	}
	return fmt.Errorf("%s/%s not found", cc.opts.ServiceDomain, key)
}

func (cc *configCenter) Raw(key string) ConfigObject {
	if val, ok := cc.rawMap[key]; ok {
		return configObject(val)
	}
	return configObject{}
}

func (cc *configCenter) Options() options {
	return cc.opts
}
