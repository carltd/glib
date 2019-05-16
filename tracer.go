package glib

import (
	gtrace "github.com/carltd/glib/trace"
)

type tracerConfig struct {
	Address string `json:"addr"`
}

func initTracer(opt tracerConfig) error {
	return gtrace.InitTracer(opt.Address, confCenter.serviceDomain, "")
}
