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
// one TTL (and on SSE reconnect).
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
	// envKubeAPIURL overrides the in-cluster API endpoint (tests / local runs).
	envKubeAPIURL = "KUBERNETES_API_URL"

	defaultScopingVerb     = "list"
	defaultScopingResource = "events"
	defaultScopingCacheTTL = 60 * time.Second

	saTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"  //nolint:gosec // path, not a credential
	saCAPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" // #nosec G101 — path, not a credential

	// sarConcurrency bounds the parallel per-namespace SubjectAccessReviews.
	sarConcurrency = 8
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
			},
		},
		tokenFile: saTokenPath,
	}, nil
}

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
func (k *kubeClient) subjectAccessReview(ctx context.Context, user string, groups []string, verb, apiGroup, resource, ns string) (bool, error) {
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
func (k *kubeClient) listNamespaces(ctx context.Context) ([]string, error) {
	var names []string
	cont := ""
	for {
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

type rbacScoper struct {
	kube     *kubeClient
	verb     string
	apiGroup string
	resource string
	ttl      time.Duration

	mu    sync.Mutex
	cache map[string]scopeEntry
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

func cacheKey(u jwtutil.UserInfo) string {
	return u.Username + "\x00" + strings.Join(u.Groups, "\x00")
}

// scopeFor returns the caller's namespace scope, from cache when fresh. Any
// resolution error propagates to the caller, which must fail closed.
func (s *rbacScoper) scopeFor(ctx context.Context, u jwtutil.UserInfo) (*nsScope, error) {
	key := cacheKey(u)
	now := time.Now()

	s.mu.Lock()
	if e, ok := s.cache[key]; ok && now.Before(e.expires) {
		s.mu.Unlock()
		return e.scope, nil
	}
	s.mu.Unlock()

	scope, err := s.resolve(ctx, u)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.cache[key] = scopeEntry{scope: scope, expires: now.Add(s.ttl)}
	s.mu.Unlock()
	return scope, nil
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

// logStatus prints the scoping posture at startup.
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
