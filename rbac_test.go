package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

// ---------------------------------------------------------------------------
// Fake Kubernetes API for SubjectAccessReview + namespace list.
//
// Access model of the fake:
//   - user "root" (or group "admins") is allowed cluster-wide;
//   - user "alice" is allowed in team-a only;
//   - user "bob" is allowed in team-b and team-c;
//   - everyone else is denied everywhere.
// ---------------------------------------------------------------------------

func fakeKubeAPI(t *testing.T, sarCount *atomic.Int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"metadata": map[string]any{"name": "team-a"}},
				{"metadata": map[string]any{"name": "team-b"}},
				{"metadata": map[string]any{"name": "team-c"}},
				{"metadata": map[string]any{"name": "krateo-system"}},
			},
		})
	})

	mux.HandleFunc("/apis/authorization.k8s.io/v1/subjectaccessreviews", func(w http.ResponseWriter, r *http.Request) {
		if sarCount != nil {
			sarCount.Add(1)
		}
		var sar struct {
			Spec struct {
				User               string   `json:"user"`
				Groups             []string `json:"groups"`
				ResourceAttributes struct {
					Verb      string `json:"verb"`
					Resource  string `json:"resource"`
					Namespace string `json:"namespace"`
				} `json:"resourceAttributes"`
			} `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&sar); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ns := sar.Spec.ResourceAttributes.Namespace
		allowed := false
		switch {
		case sar.Spec.User == "root":
			allowed = true
		case contains(sar.Spec.Groups, "admins"):
			allowed = true
		case sar.Spec.User == "alice":
			allowed = ns == "team-a"
		case sar.Spec.User == "bob":
			allowed = ns == "team-b" || ns == "team-c"
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{"allowed": allowed},
		})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func newTestScoper(srv *httptest.Server, ttl time.Duration) *rbacScoper {
	return &rbacScoper{
		kube:     &kubeClient{baseURL: srv.URL, client: srv.Client()},
		verb:     defaultScopingVerb,
		resource: defaultScopingResource,
		ttl:      ttl,
		cache:    make(map[string]scopeEntry),
	}
}

// ---------------------------------------------------------------------------
// scope resolution
// ---------------------------------------------------------------------------

func TestScopeFor_ClusterWideUserIsUnrestricted(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "root"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !scope.allowAll {
		t.Errorf("cluster-wide user should be allowAll, got %+v", scope)
	}
	if scope.restricted() {
		t.Error("allowAll scope must not be restricted")
	}
	if !scope.allows("any-namespace") || !scope.allows("") {
		t.Error("allowAll scope must allow every namespace incl. cluster-scoped events")
	}
}

func TestScopeFor_GroupGrantsClusterWide(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "eve", Groups: []string{"admins"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !scope.allowAll {
		t.Errorf("admins group should be allowAll, got %+v", scope)
	}
}

func TestScopeFor_TenantUserIsScoped(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "alice"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope.allowAll {
		t.Fatal("tenant user must not be allowAll")
	}
	if len(scope.namespaces) != 1 || scope.namespaces[0] != "team-a" {
		t.Fatalf("alice namespaces = %v, want [team-a]", scope.namespaces)
	}
	if !scope.allows("team-a") {
		t.Error("alice must see team-a")
	}
	if scope.allows("team-b") {
		t.Error("alice must NOT see team-b (another tenant)")
	}
	if scope.allows("") {
		t.Error("scoped user must NOT see cluster-scoped (namespace-less) events")
	}
}

func TestScopeFor_MultiNamespaceSorted(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "bob"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(scope.namespaces) != 2 || scope.namespaces[0] != "team-b" || scope.namespaces[1] != "team-c" {
		t.Fatalf("bob namespaces = %v, want [team-b team-c]", scope.namespaces)
	}
}

func TestScopeFor_UnknownUserSeesNothing(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	scope, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "mallory"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope.allowAll || len(scope.namespaces) != 0 {
		t.Fatalf("unknown user must have an empty scope, got %+v", scope)
	}
	if scope.allows("team-a") {
		t.Error("unknown user must not see any namespace")
	}
}

func TestScopeFor_CachesWithinTTL(t *testing.T) {
	var sarCount atomic.Int64
	s := newTestScoper(fakeKubeAPI(t, &sarCount), time.Minute)
	u := jwtutil.UserInfo{Username: "alice"}

	if _, err := s.scopeFor(context.Background(), u); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	first := sarCount.Load()
	if first == 0 {
		t.Fatal("expected SAR calls on first resolution")
	}
	if _, err := s.scopeFor(context.Background(), u); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := sarCount.Load(); got != first {
		t.Errorf("second resolution within TTL must be served from cache (SARs %d -> %d)", first, got)
	}
}

func TestScopeFor_CacheKeyIncludesGroups(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	plain, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "eve"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	admin, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "eve", Groups: []string{"admins"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plain.allowAll || !admin.allowAll {
		t.Errorf("same user with different groups must resolve independently: plain=%+v admin=%+v", plain, admin)
	}
}

func TestScopeFor_FailsClosedOnAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	s := newTestScoper(srv, time.Minute)
	if _, err := s.scopeFor(context.Background(), jwtutil.UserInfo{Username: "alice"}); err == nil {
		t.Fatal("API failure must surface as an error (fail closed), got nil")
	}
}

// ---------------------------------------------------------------------------
// SQL predicate injection into /events
// ---------------------------------------------------------------------------

func TestBuildEventsQuery_ScopedAddsNamespacePredicate(t *testing.T) {
	scope := &nsScope{namespaces: []string{"team-a", "team-b"}}
	q, params, err := buildEventsQuery(url.Values{}, scope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q, "involvedObject', 'namespace') IN {namespaces:Array(String)}") {
		t.Errorf("query must contain the bound namespace predicate:\n%s", q)
	}
	if params["namespaces"] != "['team-a','team-b']" {
		t.Errorf("namespaces param = %q, want \"['team-a','team-b']\"", params["namespaces"])
	}
	// Values are bound, never inlined into the SQL.
	if strings.Contains(q, "team-a") {
		t.Errorf("namespace values must NOT be interpolated into the SQL:\n%s", q)
	}
}

func TestBuildEventsQuery_UnscopedOmitsNamespacePredicate(t *testing.T) {
	for name, scope := range map[string]*nsScope{
		"nil scope (scoping disabled)": nil,
		"allowAll (cluster admin)":     {allowAll: true},
	} {
		t.Run(name, func(t *testing.T) {
			q, params, err := buildEventsQuery(url.Values{}, scope)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Contains(q, "{namespaces:") {
				t.Errorf("unscoped query must not contain a namespace predicate:\n%s", q)
			}
			if _, ok := params["namespaces"]; ok {
				t.Errorf("unscoped query must not bind namespaces, got %v", params)
			}
		})
	}
}

func TestBuildEventsQuery_ScopeAndCompositionCompose(t *testing.T) {
	cid := "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	scope := &nsScope{namespaces: []string{"team-a"}}
	q, params, err := buildEventsQuery(mustValues(t, "composition_id="+cid), scope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q, "{composition_id:String}") || !strings.Contains(q, "{namespaces:Array(String)}") {
		t.Errorf("both predicates must be present:\n%s", q)
	}
	if params["composition_id"] != cid || params["namespaces"] != "['team-a']" {
		t.Errorf("params = %v", params)
	}
}

func TestClickhouseArrayParam_RejectsNonDNSLabels(t *testing.T) {
	bad := []string{
		"Team-A",              // uppercase
		"a'] OR 1=1 --",       // injection attempt
		"ns_with_underscores", // invalid char
		"",                    // empty
		"-leading-dash",
	}
	for _, ns := range bad {
		t.Run(ns, func(t *testing.T) {
			if _, err := clickhouseArrayParam([]string{ns}); err == nil {
				t.Fatalf("expected error for invalid namespace %q, got nil", ns)
			}
		})
	}
	good, err := clickhouseArrayParam([]string{"a", "team-1", "krateo-system"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if good != "['a','team-1','krateo-system']" {
		t.Errorf("array literal = %q", good)
	}
}

// ---------------------------------------------------------------------------
// SSE fan-out enforcement
// ---------------------------------------------------------------------------

func TestClientWants_NamespaceScoping(t *testing.T) {
	scoped := &nsScope{namespaces: []string{"team-a"}}
	cases := []struct {
		name  string
		c     *client
		msg   sseMessage
		want  bool
		since string
	}{
		{"unscoped gets everything", &client{}, sseMessage{topic: "krateo", namespace: "team-b"}, true, ""},
		{"allowAll gets everything", &client{scope: &nsScope{allowAll: true}}, sseMessage{topic: "krateo", namespace: "team-b"}, true, ""},
		{"scoped gets own namespace", &client{scope: scoped}, sseMessage{topic: "krateo", namespace: "team-a"}, true, ""},
		{"scoped denied other namespace", &client{scope: scoped}, sseMessage{topic: "krateo", namespace: "team-b"}, false, ""},
		{"scoped denied cluster-scoped event", &client{scope: scoped}, sseMessage{topic: "krateo", namespace: ""}, false, ""},
		{"scope applies on composition topic too", &client{topic: "comp-1", scope: scoped}, sseMessage{topic: "comp-1", namespace: "team-b"}, false, ""},
		{"topic+scope both pass", &client{topic: "comp-1", scope: scoped}, sseMessage{topic: "comp-1", namespace: "team-a"}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.c.wants(tc.msg); got != tc.want {
				t.Fatalf("wants(%+v) = %v, want %v", tc.msg, got, tc.want)
			}
		})
	}
}

func TestHubBroadcastRespectsNamespaceScope(t *testing.T) {
	h := newHub()
	tenantA := &client{ch: make(chan sseMessage, 4), scope: &nsScope{namespaces: []string{"team-a"}}}
	tenantB := &client{ch: make(chan sseMessage, 4), scope: &nsScope{namespaces: []string{"team-b"}}}
	admin := &client{ch: make(chan sseMessage, 4), scope: &nsScope{allowAll: true}}
	h.register(tenantA)
	h.register(tenantB)
	h.register(admin)

	h.broadcast(sseMessage{topic: "krateo", data: []byte("a"), namespace: "team-a"})

	if got := len(tenantA.ch); got != 1 {
		t.Errorf("tenant-a client received %d msgs, want 1", got)
	}
	if got := len(tenantB.ch); got != 0 {
		t.Errorf("tenant-b client received %d msgs, want 0 (cross-tenant leak)", got)
	}
	if got := len(admin.ch); got != 1 {
		t.Errorf("admin client received %d msgs, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Request-scope resolution (fail-closed wiring)
// ---------------------------------------------------------------------------

func TestResolveRequestScope_NilScoperIsUnrestricted(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/events", nil)
	scope, err := resolveRequestScope(r, nil)
	if err != nil || scope != nil {
		t.Fatalf("nil scoper should yield (nil, nil), got (%+v, %v)", scope, err)
	}
}

func TestResolveRequestScope_MissingIdentityFails(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	r, _ := http.NewRequest(http.MethodGet, "/events", nil)
	if _, err := resolveRequestScope(r, s); err == nil {
		t.Fatal("missing verified identity must fail closed, got nil error")
	}
}

func TestResolveRequestScope_UsesContextIdentity(t *testing.T) {
	s := newTestScoper(fakeKubeAPI(t, nil), time.Minute)
	r, _ := http.NewRequest(http.MethodGet, "/events", nil)
	r = r.WithContext(withUserInfo(r.Context(), jwtutil.UserInfo{Username: "alice"}))
	scope, err := resolveRequestScope(r, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if scope.allowAll || len(scope.namespaces) != 1 || scope.namespaces[0] != "team-a" {
		t.Fatalf("scope = %+v, want alice scoped to team-a", scope)
	}
}
