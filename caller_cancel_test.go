package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// helpers: captured logs and recorded spans
// ---------------------------------------------------------------------------

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

// records returns every JSON log line written so far.
func (s *syncBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s.b.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureLogs routes the default slog logger to a buffer for the test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// findLog returns the first record with the given level and msg.
func findLog(recs []map[string]any, level, msg string) map[string]any {
	for _, r := range recs {
		if r["level"] == level && r["msg"] == msg {
			return r
		}
	}
	return nil
}

// tracedServe runs h behind otelhttp, as main does with tracing on, and returns
// the response and the one server span it recorded.
func tracedServe(t *testing.T, h http.Handler, r *http.Request) (*httptest.ResponseRecorder, sdktrace.ReadOnlySpan) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	rec := httptest.NewRecorder()
	otelhttp.NewHandler(h, "sse-proxy", otelhttp.WithTracerProvider(tp)).ServeHTTP(rec, r)
	// Only the server span: a scoped request also records rbac.scope (and,
	// on a miss, the kube spans) beneath it.
	var spans []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			spans = append(spans, s)
		}
	}
	if len(spans) != 1 {
		t.Fatalf("want 1 server span, got %d", len(spans))
	}
	return rec, spans[0]
}

func spanStatusCode(s sdktrace.ReadOnlySpan) int {
	for _, kv := range s.Attributes() {
		if kv.Key == "http.response.status_code" || kv.Key == "http.status_code" {
			return int(kv.Value.AsInt64())
		}
	}
	return 0
}

// slowServer never answers within a test: it holds each request until the
// client goes away (or 5s). It drains the body first, because net/http only
// cancels r.Context() for a gone client once the body has been consumed.
func slowServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}))
}

// requestCanceledAfter returns a request whose context is canceled after d,
// the way net/http cancels r.Context() when the client goes away.
func requestCanceledAfter(t *testing.T, target string, d time.Duration) *http.Request {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	time.AfterFunc(d, cancel)
	return httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
}

// ---------------------------------------------------------------------------
// /events
// ---------------------------------------------------------------------------

// TestHandleEvents_CallerCancelIs499 is the change itself: the client goes away
// while ClickHouse is still working. The answer is 499, the log line is INFO
// "caller canceled" with no ERROR, and the otelhttp server span records 499
// with status Unset, so the tail sampler's error-status rule does not keep the
// trace as a server failure.
//
// Mutation: delete the callerGone branch after queryEventsWindowed in
// handleEvents and this fails on all three counts (500, ERROR "clickhouse
// query failed", span status Error).
func TestHandleEvents_CallerCancelIs499(t *testing.T) {
	logs := captureLogs(t)
	f := &fakeEventsCH{delays: []time.Duration{5 * time.Second}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	h := handleEvents(config{clickhouseURL: srv.URL}, srv.Client(), nil)
	start := time.Now()
	rec, span := tracedServe(t, h, requestCanceledAfter(t, "/events", 100*time.Millisecond))
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("handler kept working after the caller left: %v", el)
	}

	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status %d, want 499", rec.Code)
	}
	if got := spanStatusCode(span); got != 499 {
		t.Fatalf("server span http.response.status_code = %d, want 499 (attrs %v)", got, span.Attributes())
	}
	if span.Status().Code != codes.Unset {
		t.Fatalf("server span status = %v, want Unset: a caller leaving is not a server error", span.Status())
	}

	recs := logs.records(t)
	if findLog(recs, "INFO", "caller canceled") == nil {
		t.Fatalf("want an INFO \"caller canceled\" line, got:\n%s", logs)
	}
	for _, r := range recs {
		if r["level"] == "ERROR" {
			t.Fatalf("a caller leaving must not log at ERROR, got %v", r)
		}
	}
}

// TestHandleEvents_ClickHouseFailureStays500 is the control: a real ClickHouse
// error with the caller still waiting keeps its 500, ERROR line and Error span.
// Without it, a handler answering 499 for every error would pass the test above.
func TestHandleEvents_ClickHouseFailureStays500(t *testing.T) {
	logs := captureLogs(t)
	f := &fakeEventsCH{errBody: "Code: 60. DB::Exception: Unknown table expression identifier 'otel_logs'. (UNKNOWN_TABLE)"}
	srv := httptest.NewServer(f)
	defer srv.Close()

	h := handleEvents(config{clickhouseURL: srv.URL}, srv.Client(), nil)
	rec, span := tracedServe(t, h, httptest.NewRequest(http.MethodGet, "/events", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("server span status = %v, want Error", span.Status())
	}
	if findLog(logs.records(t), "ERROR", "clickhouse query failed") == nil {
		t.Fatalf("want ERROR \"clickhouse query failed\", got:\n%s", logs)
	}
}

// TestHandleEvents_OwnDeadlineIsNot499: our own deadline passing is a partial
// answer (200), never mistaken for the caller leaving.
func TestHandleEvents_OwnDeadlineIsNot499(t *testing.T) {
	f := &fakeEventsCH{delays: []time.Duration{5 * time.Second}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	rec := serveEvents(t, config{clickhouseURL: srv.URL, eventsTimeout: 100 * time.Millisecond}, srv.Client(), "")
	if rec.Code != http.StatusOK || rec.Header().Get(eventsStatusHeader) != eventsStatusTimeout {
		t.Fatalf("own deadline: status %d, %s=%q; want 200 timeout", rec.Code, eventsStatusHeader, rec.Header().Get(eventsStatusHeader))
	}
}

// TestHandleEvents_CallerCancelDuringScopeResolutionIs499: the caller can also
// leave while the RBAC scope (SubjectAccessReviews) is being resolved.
func TestHandleEvents_CallerCancelDuringScopeResolutionIs499(t *testing.T) {
	logs := captureLogs(t)
	slowKube := slowServer()
	defer slowKube.Close()
	scoper := newTestScoper(slowKube, time.Minute)
	// The shared resolution outlives the caller by design (scopeFor); bound it
	// so slowKube.Close does not wait out the stub's 5s.
	scoper.resolveTimeout = 300 * time.Millisecond

	r := requestCanceledAfter(t, "/events", 100*time.Millisecond)
	r = r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: "bob"}))
	rec := httptest.NewRecorder()
	handleEvents(config{clickhouseURL: "http://127.0.0.1:1"}, nil, scoper).ServeHTTP(rec, r)
	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status %d, want 499", rec.Code)
	}
	if findLog(logs.records(t), "ERROR", "rbac scope resolution failed") != nil {
		t.Fatalf("a caller leaving during scope resolution must not log at ERROR:\n%s", logs)
	}
}

// ---------------------------------------------------------------------------
// /audit: the same settings, a deadline, and 499 on a caller leaving
// ---------------------------------------------------------------------------

func auditRequest(ctx context.Context, user string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/audit", nil).WithContext(ctx)
	return r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: user}))
}

// TestHandleAudit_SendsServerSideStopSettings pins that an /audit read carries
// readonly=2, cancel_http_readonly_queries_on_client_close=1 and a
// max_execution_time no larger than its deadline. Mutation: make
// queryAuditRecords build its own request again (as before this release) and
// all three are missing.
func TestHandleAudit_SendsServerSideStopSettings(t *testing.T) {
	ch := newClickhouseStub(t, `{"user":"bob","namespace":"team-b","action":"call","outcome":"success"}`+"\n")
	w := serveAudit(t, ch, "bob")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	p := ch.lastParams
	if p["readonly"] != "2" || p["cancel_http_readonly_queries_on_client_close"] != "1" {
		t.Fatalf("audit query lacks the cancel-on-close settings: %v", p)
	}
	met, err := strconv.ParseFloat(p["max_execution_time"], 64)
	if err != nil || met <= 0 || met > defaultAuditQueryTimeout.Seconds() {
		t.Fatalf("max_execution_time = %q, want (0, %v]", p["max_execution_time"], defaultAuditQueryTimeout.Seconds())
	}
	if p["param_namespaces"] == "" || p["param_limit"] == "" {
		t.Fatalf("bound parameters must still be sent: %v", p)
	}
	// Semantics otherwise unchanged: still no time bound on the audit query.
	if strings.Contains(ch.lastQuery, "{since:Int64}") {
		t.Fatalf("audit query gained a window it did not have:\n%s", ch.lastQuery)
	}
}

// TestHandleAudit_DeadlineStopsTheRead: a ClickHouse that never answers is cut
// at the audit deadline with a 500 (as any failure was before), not held until
// the caller gives up. Mutation: drop the context.WithTimeout in handleAudit
// and this waits the stub's full 5s.
func TestHandleAudit_DeadlineStopsTheRead(t *testing.T) {
	slow := slowServer()
	defer slow.Close()
	scoper := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	h := handleAudit(config{clickhouseURL: slow.URL, auditTimeout: 150 * time.Millisecond}, slow.Client(), scoper)

	start := time.Now()
	w := httptest.NewRecorder()
	h(w, auditRequest(context.Background(), "bob"))
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("audit read was not cut at its deadline: took %v", el)
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", w.Code)
	}
}

// TestHandleAudit_CallerCancelIs499 mirrors the /events change for /audit.
func TestHandleAudit_CallerCancelIs499(t *testing.T) {
	logs := captureLogs(t)
	slow := slowServer()
	defer slow.Close()
	scoper := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	// Resolve bob's scope once so the canceled request below spends its time
	// in ClickHouse, not in the SubjectAccessReviews.
	if _, err := scoper.scopeFor(context.Background(), jwtutil.UserInfo{Username: "bob"}); err != nil {
		t.Fatal(err)
	}
	h := handleAudit(config{clickhouseURL: slow.URL}, slow.Client(), scoper)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(100*time.Millisecond, cancel)
	rec, span := tracedServe(t, h, auditRequest(ctx, "bob"))
	if rec.Code != statusClientClosedRequest {
		t.Fatalf("status %d, want 499", rec.Code)
	}
	if span.Status().Code != codes.Unset {
		t.Fatalf("server span status = %v, want Unset", span.Status())
	}
	if findLog(logs.records(t), "ERROR", "clickhouse query failed") != nil {
		t.Fatalf("a caller leaving must not log ERROR:\n%s", logs)
	}
	if findLog(logs.records(t), "INFO", "caller canceled") == nil {
		t.Fatalf("want INFO \"caller canceled\":\n%s", logs)
	}
}

func TestConfig_AuditTimeout(t *testing.T) {
	if (config{}).auditQueryTimeout() != defaultAuditQueryTimeout {
		t.Fatal("zero config must use the default audit timeout")
	}
	if defaultAuditQueryTimeout >= 10*time.Second {
		t.Fatalf("default audit timeout %v is not below snowplow's 10s upstream timeout", defaultAuditQueryTimeout)
	}
	t.Setenv("AUDIT_QUERY_TIMEOUT", "3s")
	if got := loadConfig().auditQueryTimeout(); got != 3*time.Second {
		t.Fatalf("AUDIT_QUERY_TIMEOUT not applied: %v", got)
	}
}
