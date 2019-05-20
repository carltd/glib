package gtrace

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/server"
)

const (
	// TracePropagationField is the key for the tracing context
	// that will be injected in go-micro's metadata.
	tracePropagationField = "Tracer-Context"
)


var (
	// StatusCode is the RPC status code.
	StatusCode = "rpc.status"
	StatusOK   = "OK"

	// Service is the name of the micro-service.
	service = "rpc.service"

	// Method is the service method called.
	method = "rpc.method"
)

type clientWrapper struct {
	client.Client
}

// Call implements client.Client.Call.
func (w *clientWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) (err error) {
	var span, cCtx = tc.StartSpanFromContext(
		ctx,
		fmt.Sprintf("rpc/client/%s/%s", req.Service(), req.Method()),
	)

	span.Tag(service, req.Service())
	span.Tag(method, req.Method())

	defer func() {
		if err != nil {
			span.Tag(StatusCode, err.Error())
		} else {
			span.Tag(StatusCode, StatusOK)
		}
		span.Finish()
	}()

	// FIXME: need test
	// ctx = injectTraceIntoCtx(cCtx, span)

	err = w.Client.Call(cCtx, req, rsp, opts...)
	return
}

// Publish implements client.Client.Publish.
func (w *clientWrapper) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) (err error) {
	var span, cCtx = tc.StartSpanFromContext(
		ctx,
		fmt.Sprintf("rpc/client/pub/%s", p.Topic()),
	)
	span.Tag(service, "pub")
	span.Tag(method, p.Topic())

	defer func() {
		if err != nil {
			span.Tag(StatusCode, err.Error())
		} else {
			span.Tag(StatusCode, StatusOK)
		}
		span.Finish()
	}()

	err = w.Client.Publish(cCtx, p, opts...)
	return
}

// NewClientWrapper returns a client.Wrapper
// that adds monitoring to outgoing requests.
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

	traceCtxBytes, err := base64.RawStdEncoding.DecodeString(encodedTraceCtx)
	if err != nil {
		log.Printf("Could not decode trace context: %v", err)
		return nil
	}
	_ = traceCtxBytes
	// TODO: span.Context反序列化

	return nil
}

func injectTraceIntoCtx(ctx context.Context, sp zipkin.Span) context.Context {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}

	// TODO: span.Context序列化成 可传输格式
	sp.Context()
	md[tracePropagationField] = base64.RawStdEncoding.EncodeToString(nil)

	return metadata.NewContext(ctx, md)
}

// NewHandlerWrapper returns a server.HandlerWrapper
// that adds tracing to incoming requests.
func NewHandlerWrapper() server.HandlerWrapper {
	return func(fn server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) (err error) {

			var sp zipkin.Span
			sp, ctx = tc.StartSpanFromContext(
				ctx,
				fmt.Sprintf("rpc/server/%s/%s", req.Service(), req.Method()),
			)

			defer func() {
				if err != nil {
					sp.Tag(StatusCode, err.Error())
				} else {
					sp.Tag(StatusCode, StatusOK)
				}
				sp.Finish()
			}()
			// var spanCtx = getTraceFromCtx(ctx)
			// if spanCtx != nil {
			//
			// 	sp = tc.StartSpan(
			// 		fmt.Sprintf("rpc/server/%s/%s", req.Service(), req.Method()),
			// 		zipkin.Parent(*spanCtx),
			// 	)
			// } else {
			// 	sp = tc.StartSpan(fmt.Sprintf("rpc/server/%s/%s", req.Service(), req.Method()))
			// }

			err = fn(ctx, req, rsp)
			return
		}
	}
}

//
// // NewSubscriberWrapper returns a server.SubscriberWrapper
// // that adds tracing to subscription requests.
// func NewSubscriberWrapper() server.SubscriberWrapper {
// 	return func(fn server.SubscriberFunc) server.SubscriberFunc {
// 		return func(ctx context.Context, p server.Message) (err error) {
// 			t := newPublicationTracker(p, ServerProfile)
// 			ctx = t.start(ctx, false)
//
// 			defer func() { t.end(ctx, err) }()
//
// 			spanCtx := getTraceFromCtx(ctx)
// 			if spanCtx != nil {
// 				ctx, t.span = trace.StartSpanWithRemoteParent(
// 					ctx,
// 					fmt.Sprintf("rpc/server/pubsub/%s", p.Topic()),
// 					*spanCtx,
// 				)
// 			} else {
// 				ctx, t.span = trace.StartSpan(
// 					ctx,
// 					fmt.Sprintf("rpc/server/pubsub/%s", p.Topic()),
// 				)
// 			}
//
// 			err = fn(ctx, p)
// 			return
// 		}
// 	}
// }
