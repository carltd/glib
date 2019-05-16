package glib

import (
	// "github.com/micro/go-plugins/wrapper/trace/opencensus"
	"github.com/openzipkin/zipkin-go"

	// "github.com/openzipkin/zipkin-go/reporter"
	"github.com/openzipkin/zipkin-go/reporter/http"
	// "go.opencensus.io/exporter/zipkin"
	// "go.opencensus.io/stats/view"
	// "go.opencensus.io/trace"
)

const defaultTracerAddr = "http://host.docker.internal:9411/api/v2/spans"

var (
	tc *zipkin.Tracer
)

type tracerConfig struct {
	Address string `json:"addr"`
}

func initTracer(opt tracerConfig) error {
	// 创建本地端点 (提供的服务名、端口号)
	localEndpoint, err := zipkin.NewEndpoint(confCenter.serviceDomain, "")
	if err != nil {
		return err
	}

	if len(opt.Address) == 0 {
		opt.Address = defaultTracerAddr
	}

	// 创建提交Goroutine，并启动
	var report = http.NewReporter(opt.Address)

	// initialize the tracer
	tc, err = zipkin.NewTracer(
		report,
		zipkin.WithLocalEndpoint(localEndpoint),
		zipkin.WithSampler(zipkin.NewModuloSampler(1)),
	)
	return err

	// // The OpenCensus exporter wraps the Zipkin reporter
	// exporter := zipkin.NewExporter(rpt, localEndpoint)
	// trace.RegisterExporter(exporter)
	//
	// // For example purposes, sample every trace.
	// trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	//
	// // Register to all RPC server views.
	// if err = view.Register(opencensus.DefaultServerViews...); err != nil {
	// 	return err
	// }
	//
	// // Register to all RPC client views.
	// if err = view.Register(opencensus.DefaultClientViews...); err != nil {
	// 	return err
	// }
	// return nil
}

func closeTracer() error {
	// if rpt != nil {
	// 	return rpt.Close()
	// }
	return nil
}
