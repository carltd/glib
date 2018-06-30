package glib

import (
	"context"

	"github.com/micro/go-config"
	"github.com/micro/go-config/source/consul"
)

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
