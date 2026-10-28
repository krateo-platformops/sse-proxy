package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

// clickhouseStub records every call so a test can prove a request did, or did
// NOT, reach ClickHouse. It answers with the given JSONEachRow body.
type clickhouseStub struct {
	calls      atomic.Int64
	lastParams map[string]string
	lastQuery  string
	srv        *httptest.Server
}

func newClickhouseStub(t *testing.T, body string) *clickhouseStub {
	t.Helper()
	s := &clickhouseStub{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.calls.Add(1)
		s.lastParams = map[string]string{}
		for k, v := range r.URL.Query() {
			s.lastParams[k] = v[0]
		}
		b, _ := io.ReadAll(r.Body)
		s.lastQuery = string(b)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// serveAudit runs handleAudit as `user`, with scope resolved through the real
// resolveRequestScope -> scopeFor -> SubjectAccessReview path (fakeKubeAPI).
func serveAudit(t *testing.T, ch *clickhouseStub, user string) *httptest.ResponseRecorder {
	t.Helper()
	scoper := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	h := handleAudit(config{clickhouseURL: ch.srv.URL}, ch.srv.Client(), scoper)
	r := httptest.NewRequest(http.MethodGet, "/audit", nil)
	r = r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: user}))
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

// The C7 invariant at request time: a caller authorized for NO namespace gets
// an empty list AND no ClickHouse round-trip. Asserting only "[]" would also be
// satisfied by an empty database; the zero-call assertion is what proves the
// short-circuit (delete the empty-scope branch in handleAudit and this goes red).
func TestHandleAudit_EmptyScopeShortCircuitsWithoutQuerying(t *testing.T) {
	ch := newClickhouseStub(t, `{"user":"leak","namespace":"team-a","action":"call","outcome":"success"}`+"\n")
	w := serveAudit(t, ch, "mallory") // fakeKubeAPI: denied everywhere -> restricted, empty set

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := strings.TrimSpace(w.Body.String()); got != "[]" {
		t.Fatalf("body = %q, want []", got)
	}
	if n := ch.calls.Load(); n != 0 {
		t.Fatalf("ClickHouse was called %d time(s) for a caller with no namespaces; want 0", n)
	}
	if got := w.Header().Get(scopeHeaderName); got != "none" {
		t.Fatalf("%s = %q, want none", scopeHeaderName, got)
	}
}

// A restricted caller with namespaces DOES query, and the query carries exactly
// that caller's namespace set as the bound Array(String) parameter.
func TestHandleAudit_RestrictedScopeQueriesWithBoundNamespaces(t *testing.T) {
	ch := newClickhouseStub(t, `{"user":"bob","namespace":"team-b","action":"call","outcome":"success"}`+"\n")
	w := serveAudit(t, ch, "bob") // fakeKubeAPI: team-b and team-c

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if n := ch.calls.Load(); n != 1 {
		t.Fatalf("ClickHouse calls = %d, want 1", n)
	}
	if got := ch.lastParams["param_namespaces"]; got != "['team-b','team-c']" {
		t.Fatalf("param_namespaces = %q, want ['team-b','team-c']", got)
	}
	if !strings.Contains(ch.lastQuery, "LogAttributes['k8s.namespace.name'] IN {namespaces:Array(String)}") {
		t.Fatalf("query lacks the bound namespace predicate:\n%s", ch.lastQuery)
	}
	var rows []AuditRecord
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || len(rows) != 1 || rows[0].Namespace != "team-b" {
		t.Fatalf("response rows = %+v (err %v), want one team-b row", rows, err)
	}
	if got := w.Header().Get(scopeHeaderName); got != "team-b,team-c" {
		t.Fatalf("%s = %q, want team-b,team-c", scopeHeaderName, got)
	}
}

// A cluster-wide caller is unrestricted: one query, no namespace predicate.
func TestHandleAudit_ClusterWideCallerIsUnrestricted(t *testing.T) {
	ch := newClickhouseStub(t, "")
	w := serveAudit(t, ch, "root")

	if w.Code != http.StatusOK || ch.calls.Load() != 1 {
		t.Fatalf("status = %d, calls = %d; want 200 and 1 call", w.Code, ch.calls.Load())
	}
	if _, bound := ch.lastParams["param_namespaces"]; bound {
		t.Fatalf("a cluster-wide caller must not get a namespace predicate")
	}
	if got := w.Header().Get(scopeHeaderName); got != "*" {
		t.Fatalf("%s = %q, want *", scopeHeaderName, got)
	}
}
