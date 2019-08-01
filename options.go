package glib

import "context"

type options struct {

	// service domain for config center
	ServiceDomain string

	// config center address
	DiscoverAddr string

	// non storage, eg: db, cache
	NoStorage bool

	// listen address for server
	RunAt string

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type option func(*options)

// WithServiceDomain - set the service's domain used prefix
func WithServiceDomain(domain string) option {
	return func(o *options) {
		o.ServiceDomain = domain
	}
}

// WithDiscoverAddr - config center address
func WithDiscoverAddr(addr string) option {
	return func(o *options) {
		o.DiscoverAddr = addr
	}
}

// WithRunAt - set the server's worker address
func WithRunAt(addr string) option {
	return func(o *options) {
		o.RunAt = addr
	}
}

// WithNoStorage - none db, cache, mgo etc.
func WithNoStorage() option {
	return func(o *options) {
		o.NoStorage = true
	}
}

func newOptions(opts ...option) options {
	opt := options{
		ServiceDomain: "com.lonphy.example",
		DiscoverAddr:  "127.0.0.1:8500",
		NoStorage:     false,
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}
