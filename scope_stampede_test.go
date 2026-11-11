package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// 1.5.1: concurrent scope misses for one identity share one resolution, and
// /events and /audit have one deadline covering scope resolution and query.
// See scopeFor (rbac.go) and handleEvents (main.go).

// ---------------------------------------------------------------------------
// countingKube: the fakeKubeAPI access model (root cluster-wide, alice in
// team-a, everyone else nothing), with per-kind call counters, a per-call
// delay and injectable failures.
// ---------------------------------------------------------------------------

type countingKube struct {
	srv *httptest.Server

	delay time.Duration
	// failures is how many of the next cluster-wide SARs answer 500.
	failures atomic.Int64

	clusterSARs atomic.Int64
	nsSARs      atomic.Int64
	lists       atomic.Int64
}

var countingKubeNamespaces = []string{"team-a", "team-b", "team-c", "krateo-system"}

func newCountingKube(t *testing.T, delay time.Duration) *countingKube {
	t.Helper()
	k := &countingKube{delay: delay}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		k.lists.Add(1)
		if !k.wait(r) {
			return
		}
		items := make([]map[string]any, 0, len(countingKubeNamespaces))
		for _, ns := range countingKubeNamespaces {
			items = append(items, map[string]any{"metadata": map[string]any{"name": ns}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	})
	mux.HandleFunc("/apis/authorization.k8s.io/v1/subjectaccessreviews", func(w http.ResponseWriter, r *http.Request) {
		var sar struct {
			Spec struct {
				User               string `json:"user"`
				ResourceAttributes struct {
					Namespace string `json:"namespace"`
				} `json:"resourceAttributes"`
			} `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&sar); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ns := sar.Spec.ResourceAttributes.Namespace
		if ns == "" {
			k.clusterSARs.Add(1)
		} else {
			k.nsSARs.Add(1)
		}
		if !k.wait(r) {
			return
		}
		if ns == "" && k.failures.Load() > 0 {
			k.failures.Add(-1)
			http.Error(w, "etcdserver: request timed out", http.StatusInternalServerError)
			return
		}
		allowed := sar.Spec.User == "root" || (sar.Spec.User == "alice" && ns == "team-a")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"allowed": allowed}})
	})
	k.srv = httptest.NewServer(mux)
	t.Cleanup(k.srv.Close)
	return k
}

// wait sleeps the configured delay, or until the client goes away (so Close
// never waits out a delay). It reports whether to answer.
func (k *countingKube) wait(r *http.Request) bool {
	if k.delay <= 0 {
		return true
	}
	select {
	case <-time.After(k.delay):
		return true
	case <-r.Context().Done():
		return false
	}
}

func (k *countingKube) scoper(ttl time.Duration) *rbacScoper {
	return newTestScoper(k.srv, ttl)
}

// waitUntil polls cond until it holds or d passes.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func inflightCount(s *rbacScoper) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inflight)
}

func cachedScope(s *rbacScoper, u jwtutil.UserInfo) (*nsScope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.cache[cacheKey(u)]
	return e.scope, ok
}

// ---------------------------------------------------------------------------
// scopeFor: one resolution per key
// ---------------------------------------------------------------------------

// TestScopeFor_StampedeResolvesOnce is the defect itself: 50 concurrent misses
// for one identity. Before 1.5.1 each ran its own resolution (50 cluster SARs,
// 50 lists, 200 namespace SARs); now they share one. Mutation: in scopeFor,
// always take the !shared branch and the counts are 50/50/200.
func TestScopeFor_StampedeResolvesOnce(t *testing.T) {
	const n = 50
	k := newCountingKube(t, 20*time.Millisecond)
	s := k.scoper(time.Minute)
	alice := jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
		errs  atomic.Int64
		wrong atomic.Int64
	)
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			scope, err := s.scopeFor(context.Background(), alice)
			if err != nil {
				errs.Add(1)
				return
			}
			if scope.allowAll || len(scope.namespaces) != 1 || scope.namespaces[0] != "team-a" {
				wrong.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	if errs.Load() != 0 || wrong.Load() != 0 {
		t.Fatalf("%d callers failed and %d got a wrong scope, want every one [team-a]", errs.Load(), wrong.Load())
	}
	if got := k.clusterSARs.Load(); got != 1 {
		t.Errorf("cluster-wide SARs = %d, want 1", got)
	}
	if got := k.lists.Load(); got != 1 {
		t.Errorf("namespace lists = %d, want 1", got)
	}
	if got, want := k.nsSARs.Load(), int64(len(countingKubeNamespaces)); got != want {
		t.Errorf("namespace SARs = %d, want %d (one fan-out)", got, want)
	}
	if got := inflightCount(s); got != 0 {
		t.Errorf("%d resolutions still registered in flight after all returned", got)
	}
}

// TestScopeFor_DistinctKeysDoNotShare: the sharing is per key. Two identities
// (or one user with other groups) resolve separately, so one can never be
// handed the other's scope.
func TestScopeFor_DistinctKeysDoNotShare(t *testing.T) {
	k := newCountingKube(t, 20*time.Millisecond)
	s := k.scoper(time.Minute)
	users := []jwtutil.UserInfo{
		{Username: "alice"},
		{Username: "root"},
		{Username: "alice", Groups: []string{"other"}},
	}
	scopes := make([]*nsScope, len(users))
	var wg sync.WaitGroup
	for i, u := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sc, err := s.scopeFor(context.Background(), u)
			if err != nil {
				t.Errorf("%d: %v", i, err)
			}
			scopes[i] = sc
		}()
	}
	wg.Wait()
	if k.clusterSARs.Load() != 3 {
		t.Fatalf("cluster SARs = %d, want 3 (one per distinct key)", k.clusterSARs.Load())
	}
	if scopes[0].allowAll || !scopes[1].allowAll || scopes[2].allowAll {
		t.Fatalf("scopes crossed between keys: alice=%+v root=%+v", scopes[0], scopes[1])
	}
}

// TestScopeFor_ErrorIsSharedButNotCached: a failed resolution fails everyone
// waiting on it (fail closed), and the next call resolves again rather than
// replaying the error. Mutation: cache the entry regardless of err and the
// second call returns a nil scope with no SAR.
func TestScopeFor_ErrorIsSharedButNotCached(t *testing.T) {
	k := newCountingKube(t, 30*time.Millisecond)
	k.failures.Store(1)
	s := k.scoper(time.Minute)
	alice := jwtutil.UserInfo{Username: "alice"}

	const n = 10
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scope, err := s.scopeFor(context.Background(), alice)
			if err != nil && scope == nil {
				failed.Add(1)
			}
		}()
	}
	wg.Wait()
	if failed.Load() != n {
		t.Fatalf("%d of %d callers failed, want all: a failed resolution must never yield a scope", failed.Load(), n)
	}
	if got := k.clusterSARs.Load(); got != 1 {
		t.Fatalf("cluster SARs = %d, want 1 (the failing resolution was shared)", got)
	}
	if _, ok := cachedScope(s, alice); ok {
		t.Fatal("a failed resolution was cached")
	}

	scope, err := s.scopeFor(context.Background(), alice)
	if err != nil {
		t.Fatalf("the call after a failure must resolve again, got %v", err)
	}
	if len(scope.namespaces) != 1 || scope.namespaces[0] != "team-a" {
		t.Fatalf("scope = %+v, want [team-a]", scope)
	}
	if got := k.clusterSARs.Load(); got != 2 {
		t.Fatalf("cluster SARs = %d, want 2 (a fresh resolution)", got)
	}
}

// TestScopeFor_WaiterLeavesOthersGetTheResult: every caller waits under its
// own ctx. The caller that STARTED the resolution and a caller that joined it
// both leave early and get ctx.Err() at once; a third caller still gets the
// scope, from the same single resolution, which went on detached from the
// first caller. Mutation: resolve under the first caller's ctx (drop
// context.WithoutCancel) and the third caller gets "context canceled".
func TestScopeFor_WaiterLeavesOthersGetTheResult(t *testing.T) {
	k := newCountingKube(t, 100*time.Millisecond)
	s := k.scoper(time.Minute)
	alice := jwtutil.UserInfo{Username: "alice"}

	type result struct {
		scope *nsScope
		err   error
		took  time.Duration
	}
	call := func(ctx context.Context) <-chan result {
		ch := make(chan result, 1)
		go func() {
			t0 := time.Now()
			sc, err := s.scopeFor(ctx, alice)
			ch <- result{sc, err, time.Since(t0)}
		}()
		return ch
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := call(firstCtx)
	waitUntil(t, time.Second, "the resolution to start", func() bool { return inflightCount(s) == 1 })

	joinCtx, cancelJoin := context.WithCancel(context.Background())
	defer cancelJoin()
	joined := call(joinCtx)
	stays := call(context.Background())

	time.Sleep(10 * time.Millisecond)
	cancelFirst()
	cancelJoin()

	for name, ch := range map[string]<-chan result{"first": first, "joined": joined} {
		r := <-ch
		if !errors.Is(r.err, context.Canceled) || r.scope != nil {
			t.Fatalf("%s caller: (%+v, %v), want (nil, context.Canceled)", name, r.scope, r.err)
		}
		// The resolution takes >= 200ms (cluster SAR, list, namespace SARs at
		// 100ms each); a caller that left must not wait for it.
		if r.took > 150*time.Millisecond {
			t.Fatalf("%s caller returned %v after its ctx ended, want promptly", name, r.took)
		}
	}

	r := <-stays
	if r.err != nil {
		t.Fatalf("remaining caller: %v, want the scope (one caller leaving must not fail the others)", r.err)
	}
	if len(r.scope.namespaces) != 1 || r.scope.namespaces[0] != "team-a" {
		t.Fatalf("remaining caller scope = %+v, want [team-a]", r.scope)
	}
	if got := k.clusterSARs.Load(); got != 1 {
		t.Fatalf("cluster SARs = %d, want 1", got)
	}
	if _, ok := cachedScope(s, alice); !ok {
		t.Fatal("the shared resolution's result was not cached")
	}
}

// TestScopeFor_DetachedResolutionIsBounded: the shared resolution is detached
// from every caller, so its own bound is what stops it. A hung apiserver
// yields an error (fail closed), not a goroutine waiting forever.
func TestScopeFor_DetachedResolutionIsBounded(t *testing.T) {
	k := newCountingKube(t, 5*time.Second)
	s := k.scoper(time.Minute)
	s.resolveTimeout = 100 * time.Millisecond

	t0 := time.Now()
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "alice"})
	if !errors.Is(err, context.DeadlineExceeded) || scope != nil {
		t.Fatalf("got (%+v, %v), want (nil, deadline exceeded)", scope, err)
	}
	if el := time.Since(t0); el > time.Second {
		t.Fatalf("resolution ran %v past its 100ms bound", el)
	}
	waitUntil(t, time.Second, "the in-flight entry to clear", func() bool { return inflightCount(s) == 0 })
}

// ---------------------------------------------------------------------------
// spans
// ---------------------------------------------------------------------------

// TestScopeSpans_ChildrenOfTheServerSpanWithoutIdentity: under the otelhttp
// server span, the resolution's spans form one tree (server -> rbac.scope ->
// rbac.resolve -> kube.*), carry the cache outcome and counts, and no
// attribute anywhere holds the username or a group.
func TestScopeSpans_ChildrenOfTheServerSpanWithoutIdentity(t *testing.T) {
	tp, sr := newRecordingTP(t)
	k := newCountingKube(t, 0)
	s := k.scoper(time.Minute)
	ch := &fakeEventsCH{}
	chSrv := httptest.NewServer(ch)
	defer chSrv.Close()

	const user, group = "alice", "tenant-a-developers"
	h := otelhttp.NewHandler(handleEvents(config{clickhouseURL: chSrv.URL}, chSrv.Client(), s), "sse-proxy", otelhttp.WithTracerProvider(tp))
	serve := func() {
		r := httptest.NewRequest(http.MethodGet, "/events", nil)
		r = r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: user, Groups: []string{group}}))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
	}
	serve() // miss
	serve() // hit

	byName := map[string][]sdktrace.ReadOnlySpan{}
	for _, sp := range sr.Ended() {
		byName[sp.Name()] = append(byName[sp.Name()], sp)
		for _, kv := range sp.Attributes() {
			if v := kv.Value.Emit(); strings.Contains(v, user) || strings.Contains(v, group) {
				t.Errorf("span %q attribute %s=%q carries the caller's identity", sp.Name(), kv.Key, v)
			}
		}
	}

	var servers []sdktrace.ReadOnlySpan
	for _, sp := range sr.Ended() {
		if sp.SpanKind() == trace.SpanKindServer {
			servers = append(servers, sp)
		}
	}
	scopes := byName["rbac.scope"]
	if len(servers) != 2 || len(scopes) != 2 {
		t.Fatalf("want 2 server and 2 rbac.scope spans, got %d and %d (%v)", len(servers), len(scopes), keys(byName))
	}
	for i, want := range []string{scopeCacheMiss, scopeCacheHit} {
		if v, _ := spanAttr(scopes[i], "sse_proxy.rbac.cache"); v.AsString() != want {
			t.Errorf("rbac.scope #%d cache = %q, want %q", i, v.AsString(), want)
		}
		if scopes[i].Parent().SpanID() != servers[i].SpanContext().SpanID() {
			t.Errorf("rbac.scope #%d is not a child of its server span", i)
		}
		if v, _ := spanAttr(scopes[i], "sse_proxy.rbac.namespaces.allowed"); v.AsInt64() != 1 {
			t.Errorf("rbac.scope #%d namespaces.allowed = %v, want 1", i, v)
		}
	}

	resolves := byName["rbac.resolve"]
	if len(resolves) != 1 || resolves[0].Parent().SpanID() != scopes[0].SpanContext().SpanID() {
		t.Fatalf("want one rbac.resolve under the missing rbac.scope, got %d", len(resolves))
	}
	if v, _ := spanAttr(resolves[0], "sse_proxy.rbac.namespaces.checked"); v.AsInt64() != int64(len(countingKubeNamespaces)) {
		t.Errorf("rbac.resolve namespaces.checked = %v, want %d", v, len(countingKubeNamespaces))
	}
	resolveID := resolves[0].SpanContext().SpanID()

	sars := byName["kube.subject_access_review"]
	if len(sars) != 1+len(countingKubeNamespaces) {
		t.Fatalf("kube.subject_access_review spans = %d, want %d", len(sars), 1+len(countingKubeNamespaces))
	}
	for _, sp := range sars {
		if sp.Parent().SpanID() != resolveID || sp.SpanKind() != trace.SpanKindClient {
			t.Errorf("SAR span not a client child of rbac.resolve: kind %v", sp.SpanKind())
		}
		if v, _ := spanAttr(sp, "http.response.status_code"); v.AsInt64() != http.StatusOK {
			t.Errorf("SAR span status code = %v, want 200", v)
		}
	}
	lists := byName["kube.list_namespaces"]
	if len(lists) != 1 || lists[0].Parent().SpanID() != resolveID {
		t.Fatalf("want one kube.list_namespaces under rbac.resolve, got %d", len(lists))
	}
	if v, _ := spanAttr(lists[0], "sse_proxy.rbac.namespaces.listed"); v.AsInt64() != int64(len(countingKubeNamespaces)) {
		t.Errorf("namespaces.listed = %v, want %d", v, len(countingKubeNamespaces))
	}
}

// TestScopeSpans_JoinedCallIsShared: a caller that joins a running resolution
// records cache=shared and a link to the resolution's span.
func TestScopeSpans_JoinedCallIsShared(t *testing.T) {
	tp, sr := newRecordingTP(t)
	k := newCountingKube(t, 50*time.Millisecond)
	s := k.scoper(time.Minute)
	alice := jwtutil.UserInfo{Username: "alice"}

	ctx1, root1 := tp.Tracer("test").Start(context.Background(), "req-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.scopeFor(ctx1, alice)
	}()
	waitUntil(t, time.Second, "the resolution to start", func() bool { return inflightCount(s) == 1 })
	ctx2, root2 := tp.Tracer("test").Start(context.Background(), "req-2")
	if _, err := s.scopeFor(ctx2, alice); err != nil {
		t.Fatal(err)
	}
	<-done
	root1.End()
	root2.End()

	var shared, resolve sdktrace.ReadOnlySpan
	for _, sp := range sr.Ended() {
		switch {
		case sp.Name() == "rbac.resolve":
			resolve = sp
		case sp.Name() == "rbac.scope" && sp.Parent().SpanID() == root2.SpanContext().SpanID():
			shared = sp
		}
	}
	if shared == nil || resolve == nil {
		t.Fatal("missing the joined rbac.scope span or the rbac.resolve span")
	}
	if v, _ := spanAttr(shared, "sse_proxy.rbac.cache"); v.AsString() != scopeCacheShared {
		t.Fatalf("joined call cache = %q, want shared", v.AsString())
	}
	if l := shared.Links(); len(l) != 1 || l[0].SpanContext.SpanID() != resolve.SpanContext().SpanID() {
		t.Fatalf("joined call must link the resolution span, links = %v", l)
	}
}

func keys(m map[string][]sdktrace.ReadOnlySpan) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---------------------------------------------------------------------------
// /events: one deadline for scope resolution and the query
// ---------------------------------------------------------------------------

func eventsRequestAs(user string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/events", nil)
	return r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: user}))
}

// TestHandleEvents_SlowScopeCutsTheClickHouseBudget is the 8.2s trace, scaled
// down by 10: scope resolution takes 500ms of an 800ms request deadline, and
// the configured query budget is 600ms. The ClickHouse work is cut to what is
// left (minus the answer margin), so the partial answer arrives before the
// request deadline. Before 1.5.1 the query got its full 600ms after the
// resolution and the answer came at ~1.1s. Mutation: derive qctx from ctx with
// cfg.eventsQueryTimeout() again and the elapsed check fails.
func TestHandleEvents_SlowScopeCutsTheClickHouseBudget(t *testing.T) {
	k := newCountingKube(t, 500*time.Millisecond) // root: one cluster SAR, 500ms
	s := k.scoper(time.Minute)
	now := time.Now().Unix()
	// One row in the last hour, below the limit: the 1h window answers at
	// once and the 24h window hangs, so the deadline yields a partial answer.
	ch := &fakeEventsCH{rows: []int64{now - 10}, delays: []time.Duration{0, 10 * time.Second, 10 * time.Second}}
	chSrv := httptest.NewServer(ch)
	defer chSrv.Close()
	cfg := config{clickhouseURL: chSrv.URL, eventsReqTimeout: 800 * time.Millisecond, eventsTimeout: 600 * time.Millisecond}

	t0 := time.Now()
	rec := httptest.NewRecorder()
	handleEvents(cfg, chSrv.Client(), s).ServeHTTP(rec, eventsRequestAs("root"))
	el := time.Since(t0)

	if el >= cfg.eventsReqTimeout {
		t.Fatalf("answered after %v, want before the %v request deadline", el, cfg.eventsReqTimeout)
	}
	if el < 500*time.Millisecond {
		t.Fatalf("answered after %v, before the scope could have resolved", el)
	}
	if rec.Code != http.StatusOK || rec.Header().Get(eventsStatusHeader) != eventsStatusPartial {
		t.Fatalf("status %d, %s=%q; want 200 partial", rec.Code, eventsStatusHeader, rec.Header().Get(eventsStatusHeader))
	}
	if got := rec.Header().Get(eventsWindowHeader); got != "3600" {
		t.Fatalf("%s = %q, want the 1h window that finished", eventsWindowHeader, got)
	}
	// ClickHouse was told the cut budget, not the configured 600ms.
	ch.mu.Lock()
	met, _ := strconv.ParseFloat(ch.settings[0].Get("max_execution_time"), 64)
	ch.mu.Unlock()
	if left := (cfg.eventsReqTimeout - 500*time.Millisecond - eventsAnswerMargin).Seconds(); met <= 0 || met > left {
		t.Fatalf("max_execution_time = %v, want (0, %v]: the budget left after scope resolution", met, left)
	}
}

// TestHandleEvents_ScopePastTheDeadlineAnswersTimeoutNeverUnscoped: when the
// request deadline passes before the scope is known, the answer is the
// existing "deadline before any window finished" one (200, [], timeout, window
// 0) with a WARN, and ClickHouse is never queried: no scope, no rows. The
// resolution goes on detached and fills the cache, so the next request is a
// hit.
func TestHandleEvents_ScopePastTheDeadlineAnswersTimeoutNeverUnscoped(t *testing.T) {
	logs := captureLogs(t)
	k := newCountingKube(t, 300*time.Millisecond)
	s := k.scoper(time.Minute)
	ch := &fakeEventsCH{rows: []int64{time.Now().Unix() - 10}}
	chSrv := httptest.NewServer(ch)
	defer chSrv.Close()
	cfg := config{clickhouseURL: chSrv.URL, eventsReqTimeout: 150 * time.Millisecond}
	h := handleEvents(cfg, chSrv.Client(), s)

	t0 := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, eventsRequestAs("root"))
	if el := time.Since(t0); el > 250*time.Millisecond {
		t.Fatalf("answered after %v, want at the 150ms request deadline", el)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(eventsStatusHeader); got != eventsStatusTimeout {
		t.Fatalf("%s = %q, want timeout", eventsStatusHeader, got)
	}
	if got := rec.Header().Get(eventsWindowHeader); got != "0" {
		t.Fatalf("%s = %q, want 0", eventsWindowHeader, got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Fatalf("body = %q, want []", body)
	}
	if n := len(ch.recordedSinces()); n != 0 {
		t.Fatalf("ClickHouse was queried %d times without a resolved scope", n)
	}
	recs := logs.records(t)
	if findLog(recs, "WARN", "rbac scope resolution hit the request deadline; answered timeout with no rows") == nil {
		t.Fatalf("want a WARN for the scope deadline, got:\n%s", logs)
	}
	if findLog(recs, "ERROR", "rbac scope resolution failed") != nil {
		t.Fatalf("our own deadline is not a resolution failure:\n%s", logs)
	}

	root := jwtutil.UserInfo{Username: "root"}
	waitUntil(t, 2*time.Second, "the detached resolution to fill the cache", func() bool {
		_, ok := cachedScope(s, root)
		return ok
	})
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, eventsRequestAs("root"))
	if rec.Header().Get(eventsStatusHeader) != eventsStatusComplete {
		t.Fatalf("next request: %s = %q, want complete from the cached scope", eventsStatusHeader, rec.Header().Get(eventsStatusHeader))
	}
	if got := k.clusterSARs.Load(); got != 1 {
		t.Fatalf("cluster SARs = %d, want 1", got)
	}
}

// TestHandleEvents_ScopeFailureStays500 is the control for the test above: a
// resolution that FAILS (the apiserver errs) before the deadline is still a
// 500, not a timeout answer.
func TestHandleEvents_ScopeFailureStays500(t *testing.T) {
	k := newCountingKube(t, 0)
	k.failures.Store(1)
	ch := &fakeEventsCH{}
	chSrv := httptest.NewServer(ch)
	defer chSrv.Close()
	rec := httptest.NewRecorder()
	handleEvents(config{clickhouseURL: chSrv.URL}, chSrv.Client(), k.scoper(time.Minute)).ServeHTTP(rec, eventsRequestAs("root"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if n := len(ch.recordedSinces()); n != 0 {
		t.Fatalf("ClickHouse was queried %d times after a failed resolution", n)
	}
}

// ---------------------------------------------------------------------------
// /audit: the same end-to-end deadline
// ---------------------------------------------------------------------------

// TestHandleAudit_SlowScopeCountsAgainstTheRequestDeadline: scope resolution
// takes 300ms of a 500ms request deadline and ClickHouse never answers; the
// 500 comes at the request deadline, not 300ms + the full 400ms query budget.
// Mutation: derive the audit qctx from ctx again and this takes ~700ms.
func TestHandleAudit_SlowScopeCountsAgainstTheRequestDeadline(t *testing.T) {
	k := newCountingKube(t, 300*time.Millisecond)
	slow := slowServer()
	defer slow.Close()
	cfg := config{clickhouseURL: slow.URL, auditReqTimeout: 500 * time.Millisecond, auditTimeout: 400 * time.Millisecond}
	h := handleAudit(cfg, slow.Client(), k.scoper(time.Minute))

	t0 := time.Now()
	w := httptest.NewRecorder()
	h(w, auditRequest(context.Background(), "root"))
	el := time.Since(t0)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", w.Code)
	}
	if el < 300*time.Millisecond || el >= 600*time.Millisecond {
		t.Fatalf("answered after %v, want at the 500ms request deadline", el)
	}
}

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

func TestConfig_RequestTimeouts(t *testing.T) {
	c := config{}
	if c.eventsRequestTimeout() != defaultEventsRequestTimeout || c.auditRequestTimeout() != defaultAuditRequestTimeout {
		t.Fatal("zero config must use the default request timeouts")
	}
	for name, d := range map[string]time.Duration{"events": defaultEventsRequestTimeout, "audit": defaultAuditRequestTimeout} {
		if d >= 10*time.Second {
			t.Fatalf("default %s request timeout %v is not below snowplow's 10s upstream timeout", name, d)
		}
	}
	if defaultEventsQueryTimeout >= defaultEventsRequestTimeout {
		t.Fatalf("the query budget %v must fit inside the request deadline %v", defaultEventsQueryTimeout, defaultEventsRequestTimeout)
	}
	t.Setenv("EVENTS_REQUEST_TIMEOUT", "6s")
	t.Setenv("AUDIT_REQUEST_TIMEOUT", "5s")
	if got := loadConfig().eventsRequestTimeout(); got != 6*time.Second {
		t.Fatalf("EVENTS_REQUEST_TIMEOUT not applied: %v", got)
	}
	if got := loadConfig().auditRequestTimeout(); got != 5*time.Second {
		t.Fatalf("AUDIT_REQUEST_TIMEOUT not applied: %v", got)
	}
}

func TestEventsQueryBudget(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	c := config{eventsTimeout: 4 * time.Second}
	for _, tc := range []struct {
		name     string
		deadline time.Time
		want     time.Time
	}{
		{"plenty left: the configured budget", now.Add(8 * time.Second), now.Add(4 * time.Second)},
		{"slow scope: cut to the deadline minus the margin", now.Add(2 * time.Second), now.Add(2*time.Second - eventsAnswerMargin)},
		{"nothing left: already past", now.Add(eventsAnswerMargin / 2), now.Add(-eventsAnswerMargin / 2)},
	} {
		if got := c.eventsQueryBudget(now, tc.deadline); !got.Equal(tc.want) {
			t.Errorf("%s: budget ends %v, want %v", tc.name, got.Sub(now), tc.want.Sub(now))
		}
	}
}
