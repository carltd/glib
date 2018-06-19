package glib

import "context"

type options struct {

	// service domain for config center
	ServiceDomain string

	// config center address
	DiscoverAddr string

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type option func(*options)

func WithServiceDomain(domain string) option {
	return func(o *options) {
		o.ServiceDomain = domain
	}
}
func WithDiscoverAddr(addr string) option {
	return func(o *options) {
		o.DiscoverAddr = addr
	}
}

func newOptions(opts ...option) options {
	opt := options{
		ServiceDomain: "com.lonphy.example",
		DiscoverAddr:  "127.0.0.1:8500",
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}
