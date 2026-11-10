package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The ClickHouse client span must not read as a backend failure when the
// caller went away, and must still read as one for a real ClickHouse error or
// our own deadline. See clientspan.go.

func newRecordingTP(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, sr
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// clientSpans returns the ended spans of kind Client.
func clientSpans(sr *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			out = append(out, s)
		}
	}
	return out
}

// oneClientSpan runs one queryClickHouse call through transport under ctx and
// returns the client span it produced.
func oneClientSpan(t *testing.T, ctx context.Context, url string, rt func(tp trace.TracerProvider) http.RoundTripper) sdktrace.ReadOnlySpan {
	t.Helper()
	tp, sr := newRecordingTP(t)
	client := &http.Client{Transport: rt(tp)}
	_, _ = queryClickHouse(ctx, client, config{clickhouseURL: url}, "SELECT 1", nil)
	spans := clientSpans(sr)
	if len(spans) != 1 {
		t.Fatalf("want 1 client span, got %d", len(spans))
	}
	return spans[0]
}

func wrapped(tp trace.TracerProvider) http.RoundTripper {
	return newOutboundTransport(http.DefaultTransport, tp)
}

// plainOtelhttp is the transport as it was before: otelhttp alone.
func plainOtelhttp(tp trace.TracerProvider) http.RoundTripper {
	return otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithTracerProvider(tp))
}

func canceledAfter(d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(d, cancel)
	return ctx, cancel
}

func assertCallerCanceledSpan(t *testing.T, s sdktrace.ReadOnlySpan) {
	t.Helper()
	if s.Status().Code != codes.Unset {
		t.Fatalf("client span status = %v, want Unset for a caller that left", s.Status())
	}
	if v, ok := spanAttr(s, "sse_proxy.caller_canceled"); !ok || !v.AsBool() {
		t.Fatalf("client span lacks sse_proxy.caller_canceled=true: %v", s.Attributes())
	}
	if _, ok := spanAttr(s, errorTypeKey); ok {
		t.Fatalf("client span still carries error.type: %v", s.Attributes())
	}
}

// TestClientSpan_CallerCanceledIsUnset: the caller leaves while ClickHouse is
// still working. Mutation: build the outbound transport with plain otelhttp
// (newOutboundTransport returning otelhttp.NewTransport(base)) and the span is
// Error "context canceled" again, which the control below shows.
func TestClientSpan_CallerCanceledIsUnset(t *testing.T) {
	srv := slowServer()
	defer srv.Close()
	ctx, cancel := canceledAfter(100 * time.Millisecond)
	defer cancel()
	assertCallerCanceledSpan(t, oneClientSpan(t, ctx, srv.URL, wrapped))
}

// TestClientSpan_PlainOtelhttpMarksCancelError is the before-state control:
// the same request through otelhttp alone ends Error. Without it the test above
// could pass because cancellation never produced an Error in the first place.
func TestClientSpan_PlainOtelhttpMarksCancelError(t *testing.T) {
	srv := slowServer()
	defer srv.Close()
	ctx, cancel := canceledAfter(100 * time.Millisecond)
	defer cancel()
	s := oneClientSpan(t, ctx, srv.URL, plainOtelhttp)
	if s.Status().Code != codes.Error {
		t.Fatalf("plain otelhttp status = %v, want Error (the behaviour being corrected)", s.Status())
	}
}

// TestClientSpan_CallerCanceledWhileReadingBody: the cancel lands after the
// response headers, while the body is being read. otelhttp marks the span from
// its body wrapper on that path, which the wrapper must cover too.
func TestClientSpan_CallerCanceledWhileReadingBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ts_unix":1}`+"\n")
		w.(http.Flusher).Flush()
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	ctx, cancel := canceledAfter(150 * time.Millisecond)
	defer cancel()
	assertCallerCanceledSpan(t, oneClientSpan(t, ctx, srv.URL, wrapped))
}

// TestClientSpan_OwnDeadlineStaysError: our own deadline is a slow backend, not
// a caller leaving.
func TestClientSpan_OwnDeadlineStaysError(t *testing.T) {
	srv := slowServer()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s := oneClientSpan(t, ctx, srv.URL, wrapped)
	if s.Status().Code != codes.Error {
		t.Fatalf("deadline: client span status = %v, want Error", s.Status())
	}
	if _, ok := spanAttr(s, "sse_proxy.caller_canceled"); ok {
		t.Fatal("deadline must not be marked caller_canceled")
	}
}

// TestClientSpan_ClickHouseFailureStaysError: a ClickHouse error under a live
// context stays Error.
func TestClientSpan_ClickHouseFailureStaysError(t *testing.T) {
	srv := httptest.NewServer(&fakeEventsCH{errBody: "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)"})
	defer srv.Close()
	s := oneClientSpan(t, context.Background(), srv.URL, wrapped)
	if s.Status().Code != codes.Error {
		t.Fatalf("ClickHouse 500: client span status = %v, want Error", s.Status())
	}
	if _, ok := spanAttr(s, "sse_proxy.caller_canceled"); ok {
		t.Fatal("a ClickHouse failure must not be marked caller_canceled")
	}
}

// TestClientSpan_SuccessUnchanged: a normal answer is untouched.
func TestClientSpan_SuccessUnchanged(t *testing.T) {
	srv := httptest.NewServer(&fakeEventsCH{})
	defer srv.Close()
	s := oneClientSpan(t, context.Background(), srv.URL, wrapped)
	if s.Status().Code != codes.Unset {
		t.Fatalf("success: status = %v", s.Status())
	}
	if _, ok := spanAttr(s, "sse_proxy.caller_canceled"); ok {
		t.Fatal("a successful request must not be marked caller_canceled")
	}
	if v, ok := spanAttr(s, "http.response.status_code"); !ok || v.AsInt64() != 200 {
		t.Fatalf("otelhttp attributes must still be recorded: %v", s.Attributes())
	}
}

// TestHandleEvents_CallerCancelLeavesNoErrorSpan is the whole trace as main
// wires it: the otelhttp server span and the ClickHouse client span under one
// tracer provider. After the caller leaves, no span in the trace is Error.
func TestHandleEvents_CallerCancelLeavesNoErrorSpan(t *testing.T) {
	tp, sr := newRecordingTP(t)
	srv := slowServer()
	defer srv.Close()
	client := &http.Client{Transport: newOutboundTransport(http.DefaultTransport, tp)}
	h := otelhttp.NewHandler(handleEvents(config{clickhouseURL: srv.URL}, client, nil), "sse-proxy", otelhttp.WithTracerProvider(tp))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, requestCanceledAfter(t, "/events", 100*time.Millisecond))
	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status %d, want 499", rec.Code)
	}
	spans := sr.Ended()
	if len(spans) != 2 {
		t.Fatalf("want a server and a client span, got %d", len(spans))
	}
	for _, s := range spans {
		if s.Status().Code == codes.Error {
			t.Fatalf("%v span %q is Error after the caller left: %v", s.SpanKind(), s.Name(), s.Status())
		}
	}
	cs := clientSpans(sr)
	if len(cs) != 1 {
		t.Fatalf("want 1 client span, got %d", len(cs))
	}
	if cs[0].Parent().SpanID() != spans[len(spans)-1].SpanContext().SpanID() {
		t.Fatalf("client span is not a child of the server span")
	}
	assertCallerCanceledSpan(t, cs[0])
}
