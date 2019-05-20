package glib_test

import (
	"context"
	"testing"
	"time"

	"github.com/carltd/glib"
	com_lonphy_cl_app "github.com/carltd/glib/testdata/proto"
	"github.com/carltd/glib/trace"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/registry/consul"
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

	t.Log(glib.NewId())
}

func TestTraceClientCall(t *testing.T) {
	var err = glib.Init(
		glib.WithServiceDomain(serviceDomain),
		glib.WithDiscoverAddr(consulAddr),
	)
	if err != nil {
		t.Fatal(err)
	}

	var cw = gtrace.NewClientWrapper()

	var c = client.NewClient(
		client.Wrap(cw),
		client.Registry(consul.NewRegistry()),
	)

	var dat = com_lonphy_cl_app.User{
		Name: "lonphy",
		Age:  30,
	}

	var rsp interface{}
	var req = c.NewRequest(
		"com.lonphy.cl.app",
		"User.Test1",
		&dat,
		client.WithContentType("application/json"),
	)
	t.Logf("request's service: %s", req.Service())
	if err = c.Call(context.Background(), req, rsp); err != nil {
		t.Fatal(err)
	}
	t.Logf("response: %+v", rsp)
}

func TestMain(m *testing.M) {
	m.Run()
	time.Sleep(3 * time.Second)
}
