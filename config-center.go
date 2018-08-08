package glib

import (
	"context"

	"github.com/micro/go-config"
	"github.com/micro/go-config/source/consul"
	"time"
)

type ConfigObject interface {
	Bool(def bool) bool
	Int(def int) int
	String(def string) string
	Float64(def float64) float64
	Duration(def time.Duration) time.Duration
	StringSlice(def []string) []string
	StringMap(def map[string]string) map[string]string
	Scan(val interface{}) error
	Bytes() []byte
}

type configCenter struct {
	serviceDomain string
	conf          config.Config
}

func newConfigCenter(ctx context.Context, opts ...option) (*configCenter, error) {
	// TODO: context manager
	_ = ctx
	cc := &configCenter{}
	err := cc.Init(opts...)
	return cc, err
}

func (cc *configCenter) Init(opts ...option) error {

	options := newOptions(opts...)

	consulSrc := consul.NewSource(
		consul.WithAddress(options.DiscoverAddr),
		consul.StripPrefix(false),
		consul.WithPrefix(options.ServiceDomain),
	)
	cc.serviceDomain = options.ServiceDomain
	cc.conf = config.NewConfig()
	return cc.conf.Load(consulSrc)
}

func (cc *configCenter) String(key, defValue string) string {
	return cc.conf.Get(cc.serviceDomain, key).String(defValue)
}

func (cc *configCenter) Load(key string, v interface{}) error {
	return cc.conf.Get(cc.serviceDomain, key).Scan(v)
}

func (cc *configCenter) Raw(key string) ConfigObject {
	return cc.conf.Get(cc.serviceDomain, key)
}
