package gtrace

import (
	"context"
	"strconv"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"

	"github.com/micro/go-micro/client"
	merr "github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
)

const (
	tracePropagationField = "Tracer-Context"
)

var (
	// tagStatusCode is the RPC status code.
	tagStatusCode = zipkin.Tag("rpc.status")
	StatusOK      = "200"
)

func checkAndSetSpanTagError(sp zipkin.Span, err error) {
	if err != nil {
		if e, ok := err.(*merr.Error); ok {
			tagStatusCode.Set(sp, strconv.FormatInt(int64(e.Code), 10))
			zipkin.TagError.Set(sp, e.Detail)
		} else {
			zipkin.TagError.Set(sp, err.Error())
		}

	} else {
		tagStatusCode.Set(sp, StatusOK)
	}
}

type clientWrapper struct{ client.Client }

// Call implements client.Client.Call.
func (w *clientWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) (err error) {
	var sp, cCtx = tc.StartSpanFromContext(ctx, "rpc/client/"+req.Service()+"/"+req.Method())

	defer func() {
		checkAndSetSpanTagError(sp, err)
		sp.Finish()
	}()

	ctx = injectTraceIntoCtx(cCtx, sp)

	err = w.Client.Call(ctx, req, rsp, opts...)
	return
}

// Publish implements client.Client.Publish.
func (w *clientWrapper) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) (err error) {
	var sp, cCtx = tc.StartSpanFromContext(ctx, "rpc/pub/"+p.Topic())

	defer func() {
		checkAndSetSpanTagError(sp, err)
		sp.Finish()
	}()

	err = w.Client.Publish(cCtx, p, opts...)
	return
}

func NewClientWrapper() client.Wrapper {
	return func(c client.Client) client.Client {
		return &clientWrapper{c}
	}
}

func getTraceFromCtx(ctx context.Context) *model.SpanContext {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}

	encodedTraceCtx, ok := md[tracePropagationField]
	if !ok {
		return nil
	}

	if sCtx, ok := unmarshal([]byte(encodedTraceCtx)); ok {
		return &sCtx
	}
	return nil
}

func injectTraceIntoCtx(ctx context.Context, sp zipkin.Span) context.Context {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}

	md[tracePropagationField] = string(marshal(sp.Context()))

	return metadata.NewContext(ctx, md)
}

func NewHandlerWrapper() server.HandlerWrapper {
	return func(fn server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) (err error) {

			var sp zipkin.Span
			var spanCtx = getTraceFromCtx(ctx)
			if spanCtx != nil {
				sp, ctx = tc.StartSpanFromContext(
					ctx,
					"rpc/server/"+req.Service()+"/"+req.Method(),
					zipkin.Parent(*spanCtx),
				)
			} else {
				sp = tc.StartSpan("rpc/server/" + req.Service() + "/" + req.Method())
			}

			defer func() {
				checkAndSetSpanTagError(sp, err)
				sp.Finish()
			}()

			err = fn(ctx, req, rsp)
			return
		}
	}
}

func NewSubscriberWrapper() server.SubscriberWrapper {
	return func(fn server.SubscriberFunc) server.SubscriberFunc {
		return func(ctx context.Context, p server.Message) (err error) {
			var sp zipkin.Span
			var spanCtx = getTraceFromCtx(ctx)
			if spanCtx != nil {
				sp, ctx = tc.StartSpanFromContext(
					ctx,
					"rpc/sub/"+p.Topic(),
					zipkin.Parent(*spanCtx),
				)
			} else {
				sp = tc.StartSpan("rpc/sub/" + p.Topic())
			}

			defer func() {
				checkAndSetSpanTagError(sp, err)
				sp.Finish()
			}()

			err = fn(ctx, p)
			return
		}
	}
}
