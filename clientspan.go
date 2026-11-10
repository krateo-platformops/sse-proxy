package main

import (
	"context"
	"errors"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// The ClickHouse client span when the caller went away.
//
// otelhttp's transport ends a client span with status Error for ANY transport
// error, and also when reading the response body fails. When an inbound caller
// leaves, net/http cancels its request context, the ClickHouse request derived
// from it is aborted, and the client span ended Error "context canceled". The
// trace then read as a backend failure, and the tail sampler's error-status
// rule kept it as one, although ClickHouse did nothing wrong. The server span
// already says 499 (see statusClientClosedRequest).
//
// otelhttp has no option to decide a span's status, so this works through the
// option it does have, WithTracerProvider: the outbound transport gets a tracer
// provider whose spans remember the context the request ran under. When
// otelhttp marks such a span Error while that context reports Canceled, the
// span records sse_proxy.caller_canceled=true and stays Unset, and the
// error.type attribute otelhttp sets just before is dropped too, since by
// semconv its presence means the operation failed.
//
// Only Canceled qualifies. For a request-scoped query that means the caller
// left; for the poller it means the process is shutting down, which is no
// backend failure either. Our own deadline (/events' EVENTS_QUERY_TIMEOUT,
// /audit's AUDIT_QUERY_TIMEOUT) reports DeadlineExceeded and stays Error, and
// so does a ClickHouse failure (a non-2xx) under a live context.

// newOutboundTransport is the traced transport for ClickHouse requests: otelhttp
// with a tracer provider that leaves a caller-canceled request's span Unset.
func newOutboundTransport(base http.RoundTripper, tp trace.TracerProvider) http.RoundTripper {
	return otelhttp.NewTransport(base, otelhttp.WithTracerProvider(newCallerCancelAwareTracerProvider(tp)))
}

// callerCanceledAttr marks a client span whose request was aborted because the
// caller went away.
var callerCanceledAttr = attribute.Bool("sse_proxy.caller_canceled", true)

// errorTypeKey is the semconv error.type attribute otelhttp sets on a failed
// request.
const errorTypeKey = attribute.Key("error.type")

// requestCanceled reports whether ctx was canceled rather than timed out.
func requestCanceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

// callerCancelAwareTracerProvider wraps the tracer provider given to the
// outbound (ClickHouse) otelhttp transport.
type callerCancelAwareTracerProvider struct {
	trace.TracerProvider
}

func newCallerCancelAwareTracerProvider(tp trace.TracerProvider) trace.TracerProvider {
	return callerCancelAwareTracerProvider{TracerProvider: tp}
}

func (p callerCancelAwareTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return callerCancelAwareTracer{Tracer: p.TracerProvider.Tracer(name, opts...)}
}

type callerCancelAwareTracer struct {
	trace.Tracer
}

func (t callerCancelAwareTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := t.Tracer.Start(ctx, name, opts...)
	s := &callerCancelAwareSpan{Span: span, reqCtx: ctx}
	return trace.ContextWithSpan(ctx, s), s
}

// callerCancelAwareSpan intercepts the two calls otelhttp makes on a failed
// request. Everything else passes through. A span is used by one request, so
// it holds no lock.
type callerCancelAwareSpan struct {
	trace.Span
	reqCtx context.Context
}

func (s *callerCancelAwareSpan) SetStatus(code codes.Code, description string) {
	if code == codes.Error && requestCanceled(s.reqCtx) {
		s.Span.SetAttributes(callerCanceledAttr)
		return // stays Unset
	}
	s.Span.SetStatus(code, description)
}

func (s *callerCancelAwareSpan) SetAttributes(kv ...attribute.KeyValue) {
	if requestCanceled(s.reqCtx) {
		kept := kv[:0:0]
		for _, a := range kv {
			if a.Key != errorTypeKey {
				kept = append(kept, a)
			}
		}
		kv = kept
	}
	s.Span.SetAttributes(kv...)
}
