// Server-side multi-tenant scoping for the SSE proxy.
//
// Cross-cluster isolation is physical (each cluster runs its own observability
// stack), but INSIDE a cluster every tenant's events land in the same
// ClickHouse tables. This file derives, per authenticated caller, the set of
// namespaces that caller is actually authorized to observe — straight from
// Kubernetes RBAC — and the proxy then injects that set as a mandatory filter
// into every ClickHouse query (/events) and into the SSE broadcast fan-out
// (/notifications). The frontend is never trusted: a direct API call with a
// valid token still only sees the namespaces its RBAC allows.
//
// How authorization is derived (generic, not product-specific):
//
//  1. A SubjectAccessReview is POSTed to the local Kubernetes API for the
//     caller's username+groups (taken from the verified JWT — the same
//     identity snowplow impersonates) asking "can this subject <verb>
//     <resource> cluster-wide?". If yes, the caller is unscoped (fleet/org
//     admin) and no filter is applied.
//  2. Otherwise the proxy lists all namespaces and issues one
//     SubjectAccessReview per namespace; the allowed set becomes the filter.
//     An empty set means the caller sees nothing (fail closed).
//
// The checked verb/resource default to `list events` ("can you read events in
// this namespace?") and are configurable so deployments can key scoping off
// any RBAC rule they prefer.
//
// Results are cached per (username, groups) with a short TTL to keep the
// SubjectAccessReview volume negligible; RBAC changes are picked up within
// one TTL (and on SSE reconnect). Concurrent misses for the same key share ONE
// resolution (see scopeFor), so a burst of requests from one identity after
// its entry lapsed costs one fan-out, not one per request.
//
// The Kubernetes API is called with the proxy's own ServiceAccount using a
// minimal stdlib HTTP client (no client-go — consistent with this repo's
// no-heavy-deps ethos). The ServiceAccount needs:
//
//	create subjectaccessreviews.authorization.k8s.io  (the SAR check)
//	list   namespaces                                 (candidate enumeration)
//
// (see deploy/deployment.yaml).
//
// Scoping is OPT-IN via RBAC_SCOPING_ENABLED=true and REQUIRES auth to be
// enabled — without a verified identity there is nothing to scope by, so
// enabling scoping while auth is disabled (URL_AUTHN and JWT_JWKS_URL both
// empty) is a fatal misconfiguration (the proxy refuses to start rather than
// serving unscoped data).
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const (
	envRBACScopingEnabled  = "RBAC_SCOPING_ENABLED"
	envRBACScopingVerb     = "RBAC_SCOPING_VERB"
	envRBACScopingResource = "RBAC_SCOPING_RESOURCE"
	envRBACScopingAPIGroup = "RBAC_SCOPING_APIGROUP"
	envRBACScopingCacheTTL = "RBAC_SCOPING_CACHE_TTL"

	// /audit resolves scope with its OWN SubjectAccessReview, configured separately from /events.
	//
	// WHY. Before this, handleAudit shared the events scoper, so the RBAC rule that gated the audit
	// trail was `RBAC_SCOPING_VERB/RESOURCE` — `list events` by default. Anyone who could list a
	// namespace's events could read its audit trail, and an audit-read capability could not be told
	// apart from events access. The CMP's B2 work needs them distinguishable: audit-read is granted
	// to auditor, tenant-admin and org-admin, and NOT to everyone who can see events.
	//
	// These may name a VIRTUAL resource. RBAC permits rules on resources that do not exist as CRDs
	// and SubjectAccessReview evaluates them, so e.g. `get auditrecords.audit.krateo.io` works as a
	// capability marker with no CRD behind it.
	envAuditScopingVerb     = "AUDIT_SCOPING_VERB"
	envAuditScopingResource = "AUDIT_SCOPING_RESOURCE"
	envAuditScopingAPIGroup = "AUDIT_SCOPING_APIGROUP"
	// envKubeAPIURL overrides the in-cluster API endpoint (tests / local runs).
	envKubeAPIURL = "KUBERNETES_API_URL"

	defaultScopingVerb     = "list"
	defaultScopingResource = "events"
	defaultScopingCacheTTL = 60 * time.Second

	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"  //nolint:gosec // path, not a credential
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" // #nosec G101 — path, not a credential

	// sarConcurrency bounds the parallel per-namespace SubjectAccessReviews.
	sarConcurrency = 8

	// defaultScopeResolveTimeout bounds ONE shared scope resolution (see
	// scopeFor). It runs detached from any single caller, so something else must
	// stop it, and it is longer than any request's own deadline on purpose: a
	// slow apiserver whose resolution outlives the requests that asked for it
	// still fills the cache, so the next request for that identity is a hit. A
	// bound at or below the request deadline would cancel it every time, and a
	// resolution slower than that would never succeed. Each kube API call is
	// also bounded by the client's own 10s timeout.
	defaultScopeResolveTimeout = 30 * time.Second
)

// ---------------------------------------------------------------------------
// nsScope – the resolved authorization scope of one caller.
// ---------------------------------------------------------------------------

// nsScope is the set of namespaces a caller may observe. A nil *nsScope means
// "scoping disabled" (unrestricted); allowAll marks callers whose RBAC allows
// the check cluster-wide (org/fleet admins).
type nsScope struct {
	allowAll   bool
	namespaces []string // sorted, deduplicated
}

// allows reports whether events from the given namespace may be shown to this
// caller. Cluster-scoped events carry an empty namespace and are only visible
// to unscoped/allowAll callers — a tenant-scoped caller never sees them.
func (s *nsScope) allows(ns string) bool {
	if s == nil || s.allowAll {
		return true
	}
	for _, n := range s.namespaces {
		if n == ns {
			return true
		}
	}
	return false
}

// scopeHeaderName announces the caller's resolved scope at CONNECT time, so a browser can
// tell a legitimately-narrow bell from a broken one BEFORE any frame arrives.
//
// WHY A HEADER AND NOT A FIRST EVENT. It is readable at open time, so the portal renders
// the right empty state on first paint instead of after a silence it cannot interpret.
// This is only possible because the portal moved to fetch-SSE (installer#395): EventSource
// cannot read response headers at all. A second dividend of taking the token out of the URL.
const scopeHeaderName = "X-Krateo-Event-Scope"

// scopeHeaderValue reports the header value and whether to send the header at all.
//
//	nil scope   -> ("", false)   scoping DISABLED: send NO header
//	allowAll    -> ("*", true)
//	empty set   -> ("none", true)
//	otherwise   -> ("ns-a,ns-b", true)
//
// `none` IS AN EXPLICIT TOKEN, NOT AN EMPTY VALUE. An absent header means "this proxy does
// not scope" and an empty scope means "you may see nothing" — opposite meanings, and an
// empty header value is too easily confused with absence by anything that strips empty
// headers in between. A consumer treating a missing header as unscoped is then correct for
// an old proxy and correct for a disabled one.
//
// The list needs no sorting here: nsScope.namespaces is already sorted and deduplicated
// (see the field comment and the sort in resolve), so the value is stable across reconnects
// and a consumer cannot mistake a reshuffle for a change of scope.
func (s *nsScope) scopeHeaderValue() (string, bool) {
	if s == nil {
		return "", false
	}
	if s.allowAll {
		return "*", true
	}
	if len(s.namespaces) == 0 {
		return "none", true
	}
	return strings.Join(s.namespaces, ","), true
}

// restricted reports whether a namespace predicate must be enforced.
func (s *nsScope) restricted() bool { return s != nil && !s.allowAll }

// dnsLabelRe matches an RFC 1123 label — the only legal shape of a Kubernetes
// namespace name. Everything the scoper handles comes from the K8s API and is
// therefore already a valid label; this is defence in depth before namespace
// values are serialized into a ClickHouse Array(String) parameter.
var dnsLabelRe = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// clickhouseArrayParam serializes namespaces as a ClickHouse Array(String)
// literal suitable for binding via param_<name> (values are validated as DNS
// labels first, so no quoting/escaping beyond the literal quotes is needed).
func clickhouseArrayParam(namespaces []string) (string, error) {
	quoted := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		if !dnsLabelRe.MatchString(ns) {
			return "", fmt.Errorf("invalid namespace name %q", ns)
		}
		quoted = append(quoted, "'"+ns+"'")
	}
	return "[" + strings.Join(quoted, ",") + "]", nil
}

// ---------------------------------------------------------------------------
// Minimal in-cluster Kubernetes client (stdlib only).
// ---------------------------------------------------------------------------

type kubeClient struct {
	baseURL string
	client  *http.Client
	// tokenFile is read per request so projected ServiceAccount token rotation
	// is picked up automatically. Empty means "no bearer" (tests).
	tokenFile string
}

// newInClusterKubeClient builds a client from the standard in-cluster
// environment (KUBERNETES_SERVICE_HOST/PORT + mounted ServiceAccount token and
// CA), honouring the KUBERNETES_API_URL override for tests/local runs.
func newInClusterKubeClient() (*kubeClient, error) {
	if override := os.Getenv(envKubeAPIURL); override != "" {
		return &kubeClient{baseURL: strings.TrimRight(override, "/"), client: http.DefaultClient, tokenFile: ""}, nil
	}

	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in-cluster (KUBERNETES_SERVICE_HOST/PORT unset) and %s not provided", envKubeAPIURL)
	}

	caPEM, err := os.ReadFile(saCAPath)
	if err != nil {
		return nil, fmt.Errorf("read serviceaccount CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates parsed from %s", saCAPath)
	}

	return &kubeClient{
		baseURL: "https://" + host + ":" + port,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
				// A resolution fans out sarConcurrency SARs at once. With the
				// default of 2 idle connections per host, 6 of every 8 were
				// closed after use and the next batch paid a new TCP+TLS
				// handshake to the apiserver for each.
				MaxIdleConnsPerHost: sarConcurrency,
			},
		},
		tokenFile: saTokenPath,
	}, nil
}

// do performs one kube API request. It opens no span of its own: its callers
// (subjectAccessReview, listNamespaces) own one per operation, and do records
// the response status on it. The span never carries the path's query (a list
// continue token) or the request body (the SAR spec holds the caller's
// identity).
func (k *kubeClient) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.baseURL+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if k.tokenFile != "" {
		tok, err := os.ReadFile(k.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read serviceaccount token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kube api request: %w", err)
	}
	defer resp.Body.Close()
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read kube api response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("kube api %s %s returned %d: %s", method, path, resp.StatusCode, truncate(string(out), 256))
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// subjectAccessReview asks the API server whether the given subject can
// perform verb/resource (in namespace ns; empty ns = cluster-wide).
//
// Its span names the namespace and the outcome, NEVER the user or groups: the
// identity must not reach spans.
func (k *kubeClient) subjectAccessReview(ctx context.Context, user string, groups []string, verb, apiGroup, resource, ns string) (allowed bool, err error) {
	attrs := []attribute.KeyValue{attribute.Bool("sse_proxy.rbac.sar.cluster_wide", ns == "")}
	if ns != "" {
		attrs = append(attrs, attribute.String("k8s.namespace.name", ns))
	}
	ctx, span := startSpan(ctx, "kube.subject_access_review",
		trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	defer func() {
		if err == nil {
			span.SetAttributes(attribute.Bool("sse_proxy.rbac.sar.allowed", allowed))
		}
		endSpan(ctx, span, err)
	}()

	sar := map[string]any{
		"apiVersion": "authorization.k8s.io/v1",
		"kind":       "SubjectAccessReview",
		"spec": map[string]any{
			"user":   user,
			"groups": groups,
			"resourceAttributes": map[string]any{
				"verb":      verb,
				"group":     apiGroup,
				"resource":  resource,
				"namespace": ns,
			},
		},
	}
	body, err := json.Marshal(sar)
	if err != nil {
		return false, err
	}
	out, err := k.do(ctx, http.MethodPost, "/apis/authorization.k8s.io/v1/subjectaccessreviews", body)
	if err != nil {
		return false, err
	}
	var res struct {
		Status struct {
			Allowed bool `json:"allowed"`
		} `json:"status"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		return false, fmt.Errorf("decode SubjectAccessReview response: %w", err)
	}
	return res.Status.Allowed, nil
}

// listNamespaces returns every namespace name in the cluster (paginated).
func (k *kubeClient) listNamespaces(ctx context.Context) (names []string, err error) {
	ctx, span := startSpan(ctx, "kube.list_namespaces", trace.WithSpanKind(trace.SpanKindClient))
	pages := 0
	defer func() {
		span.SetAttributes(attribute.Int("sse_proxy.kube.pages", pages))
		if err == nil {
			span.SetAttributes(attribute.Int("sse_proxy.rbac.namespaces.listed", len(names)))
		}
		endSpan(ctx, span, err)
	}()
	cont := ""
	for {
		pages++
		path := "/api/v1/namespaces?limit=500"
		if cont != "" {
			path += "&continue=" + url.QueryEscape(cont)
		}
		out, err := k.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		var list struct {
			Metadata struct {
				Continue string `json:"continue"`
			} `json:"metadata"`
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"items"`
		}
		if err := json.Unmarshal(out, &list); err != nil {
			return nil, fmt.Errorf("decode namespace list: %w", err)
		}
		for _, it := range list.Items {
			names = append(names, it.Metadata.Name)
		}
		cont = list.Metadata.Continue
		if cont == "" {
			return names, nil
		}
	}
}

// ---------------------------------------------------------------------------
// rbacScoper – resolves + caches the per-caller namespace scope.
// ---------------------------------------------------------------------------

type scopeEntry struct {
	scope   *nsScope
	expires time.Time
}

// scopeCall is one in-flight resolution, shared by every scopeFor call that
// misses the cache for the same key while it runs. scope and err are written
// once, before done is closed, and read only after it is.
type scopeCall struct {
	done  chan struct{}
	scope *nsScope
	err   error
	// span is the resolution's own span, linked from the spans of the callers
	// that joined it.
	span trace.SpanContext
}

type rbacScoper struct {
	kube     *kubeClient
	verb     string
	apiGroup string
	resource string
	ttl      time.Duration
	// resolveTimeout bounds one shared resolution. Zero means
	// defaultScopeResolveTimeout.
	resolveTimeout time.Duration

	mu    sync.Mutex
	cache map[string]scopeEntry
	// inflight holds the resolution running for each key, if any. Created on
	// first use, so a struct literal without it works.
	inflight map[string]*scopeCall
}

// loadRBACScoper returns nil when scoping is disabled. When enabled it
// requires a reachable Kubernetes API configuration (error otherwise — the
// caller treats that as fatal: fail closed, never fall back to unscoped).
func loadRBACScoper() (*rbacScoper, error) {
	if !strings.EqualFold(getEnv(envRBACScopingEnabled, "false"), "true") {
		return nil, nil
	}
	kube, err := newInClusterKubeClient()
	if err != nil {
		return nil, fmt.Errorf("rbac scoping enabled but kubernetes api unavailable: %w", err)
	}
	ttl := defaultScopingCacheTTL
	if raw := getEnv(envRBACScopingCacheTTL, ""); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid %s %q", envRBACScopingCacheTTL, raw)
		}
		ttl = d
	}
	return &rbacScoper{
		kube:     kube,
		verb:     getEnv(envRBACScopingVerb, defaultScopingVerb),
		apiGroup: getEnv(envRBACScopingAPIGroup, ""),
		resource: getEnv(envRBACScopingResource, defaultScopingResource),
		ttl:      ttl,
		cache:    make(map[string]scopeEntry),
	}, nil
}

// loadAuditScoper returns the scoper that /audit must use.
//
// IT RETURNS THE EVENTS SCOPER UNCHANGED when no AUDIT_SCOPING_* variable is set. That is what
// makes this change inert on every existing deployment: same instance, same SAR, same cache, so
// /audit behaves exactly as before until an operator configures it.
//
// A SEPARATE CACHE IS MANDATORY once it diverges, and this is the part that would be a silent
// authorization bug if shared. cacheKey is username+groups ONLY — it does not include the verb,
// resource or apiGroup. Two scopers asking DIFFERENT SubjectAccessReviews about the SAME caller
// would therefore collide on one cache entry, and whichever resolved first would answer for both:
// an events permission could be served as an audit scope, or the reverse. So a diverging audit
// scoper gets its own map.
//
// kube and ttl are shared deliberately: the API client is stateless for this purpose and a second
// TTL knob would be configuration nobody asked for.
//
// Returns nil when events scoping is disabled — there is nothing to scope, and /audit must not
// invent an enforcement the rest of the proxy is not applying.
func loadAuditScoper(events *rbacScoper) *rbacScoper {
	if events == nil {
		return nil
	}
	verb := getEnv(envAuditScopingVerb, "")
	resource := getEnv(envAuditScopingResource, "")
	apiGroup := getEnv(envAuditScopingAPIGroup, "")
	if verb == "" && resource == "" && apiGroup == "" {
		return events
	}
	// A PARTIAL configuration inherits the events value for whatever is unset, rather than falling
	// back to a built-in default. Setting only AUDIT_SCOPING_RESOURCE should change the resource and
	// nothing else; silently resetting the verb to "list" would be a surprise.
	if verb == "" {
		verb = events.verb
	}
	if resource == "" {
		resource = events.resource
	}
	if apiGroup == "" {
		apiGroup = events.apiGroup
	}
	return &rbacScoper{
		kube:     events.kube,
		verb:     verb,
		apiGroup: apiGroup,
		resource: resource,
		ttl:      events.ttl,
		cache:    make(map[string]scopeEntry),
	}
}

func cacheKey(u jwtutil.UserInfo) string {
	return u.Username + "\x00" + strings.Join(u.Groups, "\x00")
}

// Values of the sse_proxy.rbac.cache span attribute on rbac.scope.
const (
	scopeCacheHit    = "hit"    // a fresh cache entry answered
	scopeCacheMiss   = "miss"   // this call started the resolution
	scopeCacheShared = "shared" // this call joined a resolution already running
)

// scopeFor returns the caller's namespace scope, from cache when fresh. Any
// resolution error propagates to the caller, which must fail closed.
//
// ONE RESOLUTION PER KEY AT A TIME. Before 1.5.1 every miss ran resolve()
// itself. On krateo-057 (2026-11-11, load step 25 -> 50 virtual users sharing
// 3 identities) the cache entries had lapsed in an idle gap, so the burst of
// /events that followed ran about 16 identical fan-outs per identity at once
// (a cluster-wide SAR, a namespace list, a SAR per namespace each), and 31
// requests took 11.8-12.8 s, past snowplow's 10 s, with ClickHouse idle. Now
// the first miss starts the resolution and every concurrent miss for the same
// key waits for that one.
//
// The shared resolution:
//   - runs on a context detached from every caller (context.WithoutCancel of
//     the first one's, for its trace and values) under its own bound
//     (resolveTimeout). One caller leaving must not fail everyone who joined.
//   - is cached only on success. An error is returned to everyone waiting for
//     it and then forgotten, so the next call resolves again.
//
// Each caller waits under its OWN ctx: when that ends, it returns ctx.Err()
// at once and the resolution carries on for the others.
//
// The cache key is the identity only (see loadAuditScoper), so the in-flight
// map lives on the scoper next to its cache: a diverging audit scoper has its
// own of both, and never joins an events resolution.
func (s *rbacScoper) scopeFor(ctx context.Context, u jwtutil.UserInfo) (scope *nsScope, err error) {
	ctx, span := startSpan(ctx, "rbac.scope")
	defer func() {
		if err == nil {
			setScopeAttrs(span, scope)
		}
		endSpan(ctx, span, err)
	}()

	key := cacheKey(u)

	s.mu.Lock()
	if e, ok := s.cache[key]; ok && time.Now().Before(e.expires) {
		s.mu.Unlock()
		span.SetAttributes(attribute.String("sse_proxy.rbac.cache", scopeCacheHit))
		return e.scope, nil
	}
	if s.inflight == nil {
		s.inflight = make(map[string]*scopeCall)
	}
	c, shared := s.inflight[key]
	if !shared {
		c = s.startResolve(ctx, key, u)
		s.inflight[key] = c
	}
	s.mu.Unlock()

	if shared {
		span.SetAttributes(attribute.String("sse_proxy.rbac.cache", scopeCacheShared))
		span.AddLink(trace.Link{SpanContext: c.span})
	} else {
		span.SetAttributes(attribute.String("sse_proxy.rbac.cache", scopeCacheMiss))
	}

	select {
	case <-c.done:
		if c.err != nil {
			return nil, c.err
		}
		return c.scope, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startResolve starts the shared resolution for key and returns its call. It
// is called with s.mu held; the resolution takes s.mu only once it is done.
func (s *rbacScoper) startResolve(parent context.Context, key string, u jwtutil.UserInfo) *scopeCall {
	timeout := s.resolveTimeout
	if timeout <= 0 {
		timeout = defaultScopeResolveTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	ctx, span := startSpan(ctx, "rbac.resolve")
	c := &scopeCall{done: make(chan struct{}), span: span.SpanContext()}

	go func() {
		defer cancel()
		started := time.Now()
		scope, err := s.resolveRecovering(ctx, u)
		if err == nil {
			setScopeAttrs(span, scope)
		}
		endSpan(ctx, span, err)

		s.mu.Lock()
		if err == nil {
			// The TTL counts from when the SARs were asked, not answered, so a
			// slow resolution never extends how stale a scope can be.
			s.cache[key] = scopeEntry{scope: scope, expires: started.Add(s.ttl)}
		}
		delete(s.inflight, key)
		c.scope, c.err = scope, err
		s.mu.Unlock()
		close(c.done)
	}()
	return c
}

// resolveRecovering is resolve with a panic turned into an error. resolve runs
// on its own goroutine now, out of reach of net/http's per-request recovery,
// where a panic would end the process rather than one request. Fail closed
// either way: an error yields no scope.
func (s *rbacScoper) resolveRecovering(ctx context.Context, u jwtutil.UserInfo) (scope *nsScope, err error) {
	defer func() {
		if r := recover(); r != nil {
			scope, err = nil, fmt.Errorf("rbac scope resolution panicked: %v", r)
		}
	}()
	return s.resolve(ctx, u)
}

func (s *rbacScoper) resolve(ctx context.Context, u jwtutil.UserInfo) (*nsScope, error) {
	// Cluster-wide short-circuit: org/fleet admins skip per-namespace checks.
	allowedAll, err := s.kube.subjectAccessReview(ctx, u.Username, u.Groups, s.verb, s.apiGroup, s.resource, "")
	if err != nil {
		return nil, err
	}
	if allowedAll {
		return &nsScope{allowAll: true}, nil
	}

	namespaces, err := s.kube.listNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("sse_proxy.rbac.namespaces.checked", len(namespaces)))

	var (
		mu       sync.Mutex
		firstErr error
		allowed  []string
		wg       sync.WaitGroup
		sem      = make(chan struct{}, sarConcurrency)
	)
	for _, ns := range namespaces {
		wg.Add(1)
		sem <- struct{}{}
		go func(ns string) {
			defer wg.Done()
			defer func() { <-sem }()
			ok, err := s.kube.subjectAccessReview(ctx, u.Username, u.Groups, s.verb, s.apiGroup, s.resource, ns)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if ok {
				allowed = append(allowed, ns)
			}
		}(ns)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr // fail closed: a partial answer must never widen access
	}
	sort.Strings(allowed)
	return &nsScope{namespaces: allowed}, nil
}

// ---------------------------------------------------------------------------
// Spans. They take their tracer from the span already in ctx (the otelhttp
// server span when tracing is on). With tracing off there is no span in ctx,
// so these are no-ops, like the rest of the gated telemetry. NO span here may
// carry the caller's identity (username, groups) or any token.
// ---------------------------------------------------------------------------

func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return trace.SpanFromContext(ctx).TracerProvider().Tracer(meterName).Start(ctx, name, opts...)
}

// endSpan ends span with err's status. A caller that went away (ctx
// Canceled) is not a failure: the span stays Unset with
// sse_proxy.caller_canceled=true, as the ClickHouse client span does
// (clientspan.go). Our own deadline and real failures are Error.
func endSpan(ctx context.Context, span trace.Span, err error) {
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled) && requestCanceled(ctx):
		span.SetAttributes(callerCanceledAttr)
	default:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// setScopeAttrs describes a resolved scope by its shape only: allow-all, or
// how many namespaces it allows. Never whose scope it is.
func setScopeAttrs(span trace.Span, scope *nsScope) {
	if scope == nil {
		return
	}
	span.SetAttributes(
		attribute.Bool("sse_proxy.rbac.allow_all", scope.allowAll),
		attribute.Int("sse_proxy.rbac.namespaces.allowed", len(scope.namespaces)),
	)
}

// logStatus prints the scoping posture at startup.
// logAuditStatus states which RBAC rule gates the audit trail, and whether it differs from events.
// A reader must be able to tell "audit shares the events permission" from "audit has its own"
// without inspecting env, because the two have very different security properties.
func logAuditStatus(audit, events *rbacScoper) {
	switch {
	case audit == nil:
		slogInfo("sse-proxy", "audit scoping DISABLED (rbac scoping is off)")
	case audit == events:
		slogInfo("sse-proxy", "audit scoping SHARES the events permission — anyone who can "+
			envRBACScopingVerb+" "+envRBACScopingResource+" can read the audit trail; set "+
			envAuditScopingVerb+"/"+envAuditScopingResource+"/"+envAuditScopingAPIGroup+" to separate them",
			slog.String("verb", audit.verb),
			slog.String("resource", audit.resource),
			slog.String("apiGroup", audit.apiGroup),
		)
	default:
		slogInfo("sse-proxy", "audit scoping ENABLED with its OWN permission (SubjectAccessReview-derived, fail-closed)",
			slog.String("verb", audit.verb),
			slog.String("resource", audit.resource),
			slog.String("apiGroup", audit.apiGroup),
			slog.String("cache_ttl", audit.ttl.String()),
		)
	}
}

func (s *rbacScoper) logStatus() {
	if s == nil {
		slogInfo("sse-proxy", "rbac namespace scoping DISABLED — set "+envRBACScopingEnabled+"=true to enforce per-tenant filtering")
		return
	}
	slogInfo("sse-proxy", "rbac namespace scoping ENABLED (SubjectAccessReview-derived, fail-closed)",
		// what RBAC rule gates visibility: <verb> <resource>[.<apiGroup>]
		slog.String("verb", s.verb),
		slog.String("resource", s.resource),
		slog.String("apiGroup", s.apiGroup),
		slog.String("cache_ttl", s.ttl.String()),
	)
}
