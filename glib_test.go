package glib_test

import (
	"testing"

	"github.com/carltd/glib"
)

const (
	consulAddr    = "127.0.0.1:8500"
	serviceDomain = "test"
)

func TestInit(t *testing.T) {
	var err = glib.Init(
		glib.WithServiceDomain(serviceDomain),
		glib.WithDiscoverAddr(consulAddr),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = glib.Destroy(); err != nil {
		t.Fatal(err)
	}
}
