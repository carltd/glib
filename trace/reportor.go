package gtrace

import (
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/http"
)

const defaultTracerAddr = "http://host.docker.internal:9411/api/v2/spans"

var (
	tc *zipkin.Tracer
)

func InitTracer(addr, serviceName, hostPort string) error {
	ep, err := zipkin.NewEndpoint(serviceName, hostPort)
	if err != nil {
		return err
	}

	if len(addr) == 0 {
		addr = defaultTracerAddr
	}

	var report = http.NewReporter(addr)

	// initialize the tracer
	tc, err = zipkin.NewTracer(
		report,
		zipkin.WithLocalEndpoint(ep),
		zipkin.WithSampler(zipkin.NewModuloSampler(1)), // always sampler
	)
	return err
}
