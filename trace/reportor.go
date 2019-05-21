package gtrace

import (
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter/http"
)

const defaultTracerAddr = "http://host.docker.internal:9411/api/v2/spans"

var (
	tc *zipkin.Tracer
)

type TracerConfig struct {
	Address   string `json:"addr"`
	SampleMod uint64 `json:"sample_mod"`

	HostPort string `json:"-"`
	SrvName  string `json:"-"`
}

func InitTracer(opt TracerConfig) error {
	ep, err := zipkin.NewEndpoint(opt.SrvName, opt.HostPort)
	if err != nil {
		return err
	}

	if len(opt.Address) == 0 {
		opt.Address = defaultTracerAddr
	}

	var report = http.NewReporter(opt.Address)

	// initialize the tracer
	tc, err = zipkin.NewTracer(
		report,
		zipkin.WithLocalEndpoint(ep),
		zipkin.WithSampler(zipkin.NewModuloSampler(opt.SampleMod)), // always sampler
	)
	return err
}
