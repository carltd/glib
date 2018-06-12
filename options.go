package glib

import "context"

type Options struct {

	// service domain for config center
	ServiceDomain string

	// config center address
	DiscoverAddr string

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type Option func(*Options)

func WithServiceDomain(domain string) Option {
	return func(o *Options) {
		o.ServiceDomain = domain
	}
}
func WithDiscoverAddr(addr string) Option {
	return func(o *Options) {
		o.DiscoverAddr = addr
	}
}

func newOptions(opts ...Option) Options {
	opt := Options{
		ServiceDomain: "com.lonphy.example",
		DiscoverAddr:  "127.0.0.1:8500",
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}
