// Package main implements a thin SSE proxy that polls ClickHouse for new
// Kubernetes events and broadcasts them to connected browser clients using
// the Server-Sent Events (SSE) protocol.
//
// The Krateo frontend EventList widget connects to /notifications/ and listens
// for SSE messages whose `event:` field matches the compositionId. Each SSE
// message `data:` field contains a single SSEK8sEvent JSON object.
//
// Endpoints:
//
//	GET /events         JSON snapshot of recent events (SSEK8sEvent[]). Optional
//	                    query params: composition_id (UUID; server-side filter)
//	                    and limit (1..200, default 200).
//	GET /notifications  the SSE stream. Optional composition_id query param
//	                    subscribes to a single composition's events; absent ⇒
//	                    the global firehose (client filters by SSE event name).
//	GET /health         unauthenticated liveness/readiness probe.
//
// /events and /notifications validate a krateo JWT the same way snowplow does
// (RS256 against authn's JWKS public key, derived from URL_AUTHN); see
// auth.go. With RBAC_SCOPING_ENABLED=true both endpoints additionally enforce
// server-side multi-tenant scoping: the caller's authorized-namespace set is
// derived from Kubernetes RBAC (SubjectAccessReview) and injected as a
// mandatory filter into the ClickHouse query and the SSE fan-out; see rbac.go.
// The only external dependency is the krateo plumbing JWT helper
// (github.com/krateo-platformops/plumbing) plus its golang-jwt transitive; the
// ClickHouse polling + SSE hub + K8s API client remain pure standard library.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ---------------------------------------------------------------------------
// SSEK8sEvent – the JSON structure expected by the Krateo frontend EventList.
// ---------------------------------------------------------------------------

// SSEK8sEvent matches the TypeScript interface in the Krateo frontend.
type SSEK8sEvent struct {
	Metadata struct {
		Name              string `json:"name"`
		Namespace         string `json:"namespace"`
		UID               string `json:"uid"`
		CreationTimestamp string `json:"creationTimestamp"`
	} `json:"metadata"`
	InvolvedObject struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		Namespace  string `json:"namespace"`
		UID        string `json:"uid"`
	} `json:"involvedObject"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Type           string `json:"type"` // "Normal" | "Warning"
	FirstTimestamp string `json:"firstTimestamp"`
	LastTimestamp  string `json:"lastTimestamp"`
	EventTime      string `json:"eventTime"`
	Source         struct {
		Component string `json:"component"`
	} `json:"source"`
	// CompositionID is the krateo.io/composition-id the event belongs to (empty
	// for cluster-wide events that carry no composition). Exposed so clients can
	// see/scope events by composition. Camel-cased to match the rest of this
	// payload (which mirrors the frontend SSEK8sEvent/EventV1 TS type).
	CompositionID string `json:"compositionId"`
}

// ---------------------------------------------------------------------------
// chRow – a row returned by the ClickHouse polling query (JSONEachRow format).
// Fields are extracted from the raw K8s event JSON stored in otel_logs.Body
// by the k8sobjects receiver, and the composition ID is enriched by the
// compositionresolver OTel processor.
// ---------------------------------------------------------------------------

type chRow struct {
	TsUnix          int64  `json:"ts_unix"`
	CompositionID   string `json:"composition_id"`
	ObjAPIVersion   string `json:"obj_apiversion"`
	ObjName         string `json:"obj_name"`
	ObjNamespace    string `json:"obj_namespace"`
	ObjUID          string `json:"obj_uid"`
	ObjKind         string `json:"obj_kind"`
	Reason          string `json:"reason"`
	Message         string `json:"message"`
	Type            string `json:"type"`
	EventTime       string `json:"event_time"`
	SourceComponent string `json:"source_component"`
}

func (row chRow) toSSEK8sEvent() SSEK8sEvent {
	var evt SSEK8sEvent
	evt.Metadata.Name = row.ObjName
	evt.Metadata.Namespace = row.ObjNamespace
	evt.Metadata.UID = row.ObjUID
	evt.Metadata.CreationTimestamp = row.EventTime
	evt.InvolvedObject.APIVersion = row.ObjAPIVersion
	evt.InvolvedObject.Kind = row.ObjKind
	evt.InvolvedObject.Name = row.ObjName
	evt.InvolvedObject.Namespace = row.ObjNamespace
	evt.InvolvedObject.UID = row.ObjUID
	evt.Reason = row.Reason
	evt.Message = row.Message
	evt.Type = row.Type
	evt.FirstTimestamp = row.EventTime
	evt.LastTimestamp = row.EventTime
	evt.EventTime = row.EventTime
	evt.Source.Component = row.SourceComponent
	evt.CompositionID = row.CompositionID
	return evt
}

// ---------------------------------------------------------------------------
// Hub – fan-out SSE messages to all connected clients.
// ---------------------------------------------------------------------------

type sseMessage struct {
	topic string
	data  []byte
	// namespace is the K8s namespace of the involved object (empty for
	// cluster-scoped events). The hub uses it to enforce per-client RBAC
	// namespace scoping at fan-out time (see rbac.go).
	namespace string
}

type client struct {
	ch chan sseMessage
	// topic is the single topic this client is subscribed to. Empty means
	// "all topics" (the global firehose): the client receives every message,
	// preserving the original behaviour where the browser filters client-side
	// by SSE event name. A non-empty topic means the client receives ONLY
	// messages broadcast under that exact topic (server-side per-composition
	// subscription).
	topic string
	// scope is the caller's RBAC-derived namespace scope, resolved at connect
	// time (nil = scoping disabled). Enforced server-side on every message so
	// a direct connection can never stream another tenant's events.
	scope *nsScope
}

// wants reports whether this client should receive the message: the topic must
// match the subscription AND the event's namespace must be within the caller's
// RBAC-derived scope.
func (c *client) wants(msg sseMessage) bool {
	if c.topic != "" && c.topic != msg.topic {
		return false
	}
	return c.scope.allows(msg.namespace)
}

type hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[*client]struct{})}
}

func (h *hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	close(c.ch)
}

func (h *hub) broadcast(msg sseMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.wants(msg) {
			continue
		}
		// Non-blocking send: drop the message for slow clients rather than blocking.
		select {
		case c.ch <- msg:
		default:
		}
	}
}

func (h *hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type config struct {
	clickhouseURL      string
	clickhouseUser     string
	clickhousePassword string
	listenAddr         string
}

func loadConfig() config {
	return config{
		clickhouseURL:      getEnv("CLICKHOUSE_URL", "http://krateo-clickstack-clickhouse.clickhouse-system.svc:8123"),
		clickhouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		clickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		listenAddr:         getEnv("LISTEN_ADDR", ":8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------------------
// Poller – periodically queries ClickHouse and broadcasts new events.
// ---------------------------------------------------------------------------

// pollSQL fetches K8s events from otel_logs that arrived after lastSeenUnix.
// Events are stored as raw JSON in Body by the k8sobjects receiver.
// The compositionresolver processor enriches LogAttributes with krateo.io/composition-id.
// %%Y, %%m etc. become %Y, %m after fmt.Sprintf substitutes %d for the timestamp.
const pollSQL = `SELECT
    toUnixTimestamp(Timestamp)                                                      AS ts_unix,
    ifNull(LogAttributes['krateo.io/composition-id'], '')                           AS composition_id,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'apiVersion'), '')   AS obj_apiversion,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'name'), '')         AS obj_name,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'namespace'), '')    AS obj_namespace,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'uid'), '')          AS obj_uid,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'kind'), '')         AS obj_kind,
    ifNull(JSONExtractString(Body, 'object', 'reason'), '')                         AS reason,
    ifNull(JSONExtractString(Body, 'object', 'message'), '')                        AS message,
    ifNull(JSONExtractString(Body, 'object', 'type'), 'Normal')                     AS type,
    coalesce(
        nullIf(JSONExtractString(Body, 'object', 'eventTime'), ''),
        nullIf(JSONExtractString(Body, 'object', 'lastTimestamp'), ''),
        formatDateTime(toDateTime(Timestamp), '%%Y-%%m-%%dT%%H:%%i:%%SZ', 'UTC')
    )                                                                                AS event_time,
    ifNull(JSONExtractString(Body, 'object', 'source', 'component'), '')            AS source_component
FROM otel_logs
WHERE ResourceAttributes['telemetry.source'] = 'k8s-events'
  AND JSONExtractString(Body, 'object', 'reason') != ''
  AND toUnixTimestamp(Timestamp) > %d
ORDER BY Timestamp ASC
LIMIT 500
FORMAT JSONEachRow`

type poller struct {
	cfg          config
	hub          *hub
	metrics      *proxyMetrics
	client       *http.Client
	lastSeenUnix int64
}

func newPoller(cfg config, h *hub, m *proxyMetrics, client *http.Client) *poller {
	// Initialise to one hour ago to surface recent events on startup.
	return &poller{
		cfg:          cfg,
		hub:          h,
		metrics:      m,
		client:       client,
		lastSeenUnix: time.Now().Add(-1 * time.Hour).Unix(),
	}
}

func (p *poller) run(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.hub.count() == 0 {
				// No clients connected – skip the poll to avoid unnecessary load.
				continue
			}
			p.poll(ctx)
		}
	}
}

func (p *poller) poll(ctx context.Context) {
	query := fmt.Sprintf(pollSQL, p.lastSeenUnix)
	rows, err := queryClickHouse(ctx, p.client, p.cfg, query, nil)
	if err != nil {
		p.metrics.incPollError(ctx)
		slogErrorCtx(ctx, "poller", "clickhouse query failed", err)
		return
	}

	maxTs := p.lastSeenUnix
	var delivered int64
	for _, row := range rows {
		if row.TsUnix > maxTs {
			maxTs = row.TsUnix
		}

		evt := row.toSSEK8sEvent()
		data, err := json.Marshal(evt)
		if err != nil {
			slogErrorCtx(ctx, "poller", "event marshal failed", err)
			continue
		}

		// Global topic — mirrors eventsse behaviour where all events
		// are published under the "krateo" topic. The event's namespace rides
		// along so the hub can enforce per-client RBAC scoping at fan-out.
		p.hub.broadcast(sseMessage{topic: "krateo", data: data, namespace: row.ObjNamespace})
		delivered++

		// Composition-specific topic so per-composition listeners
		// only receive their own events.
		if row.CompositionID != "" {
			p.hub.broadcast(sseMessage{topic: row.CompositionID, data: data, namespace: row.ObjNamespace})
		}
	}

	if maxTs > p.lastSeenUnix {
		p.lastSeenUnix = maxTs
	}
	if len(rows) > 0 {
		p.metrics.addEventsDelivered(ctx, delivered)
		slogInfoCtx(ctx, "poller", "broadcasted events",
			slog.Int("count", len(rows)),
			slog.Int64("last_seen", p.lastSeenUnix),
		)
	}
}

// ---------------------------------------------------------------------------
// Shared ClickHouse query helper
// ---------------------------------------------------------------------------

// queryClickHouse runs query against ClickHouse over HTTP. Any params are
// passed as ClickHouse query parameters (param_<name>=<value> on the request
// URL) so the SQL can reference them safely as {name:Type} placeholders —
// values are bound by ClickHouse, never string-concatenated into the SQL.
func queryClickHouse(ctx context.Context, client *http.Client, cfg config, query string, params map[string]string) ([]chRow, error) {
	if client == nil {
		client = http.DefaultClient
	}
	endpoint := cfg.clickhouseURL
	if len(params) > 0 {
		u, err := url.Parse(cfg.clickhouseURL)
		if err != nil {
			return nil, fmt.Errorf("parse clickhouse url: %w", err)
		}
		q := u.Query()
		for k, v := range params {
			q.Set("param_"+k, v)
		}
		u.RawQuery = q.Encode()
		endpoint = u.String()
	}

	// NewRequestWithContext so the otelhttp transport (when installed) can
	// create a client span and inject the W3C traceparent into the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")
	if cfg.clickhouseUser != "" {
		req.SetBasicAuth(cfg.clickhouseUser, cfg.clickhousePassword)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse returned %d: %s", resp.StatusCode, body)
	}

	var rows []chRow
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var row chRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			slogErrorCtx(ctx, "query", "row unmarshal failed", err, slog.String("line", line))
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// REST /events – returns recent K8s events as a plain JSON array of EventV1.
// The frontend's useGetEvents hook calls EVENTS_API_BASE_URL + "/events" and
// expects (await res.json()) as SSEK8sEvent[].
//
// Query parameters (all optional):
//   - composition_id: when present, only events for that composition are
//     returned (server-side filter). MUST be a UUID. Absent ⇒ all events
//     (the dashboard "all activity" view, the original behaviour).
//   - limit: max rows to return; defaults to and is capped at defaultEventsLimit.
//
// SQL-injection safety: composition_id and limit are NEVER concatenated into
// the SQL. composition_id is strictly validated as a UUID and then bound as a
// ClickHouse query parameter ({composition_id:String}); limit is parsed to a
// bounded integer and bound as {limit:UInt32}.
// ---------------------------------------------------------------------------

// defaultEventsLimit is both the default and the hard cap for /events.
const defaultEventsLimit = 200

// compositionPredicatePlaceholder is replaced (via strings.Replace, NOT
// fmt.Sprintf — the template contains literal ClickHouse % format directives)
// with either an empty string or compositionIDFilter.
const compositionPredicatePlaceholder = "/*COMPOSITION_PREDICATE*/"

// namespacePredicatePlaceholder is replaced with either an empty string (no
// scoping / cluster-wide caller) or namespaceFilter (RBAC-scoped caller).
const namespacePredicatePlaceholder = "/*NAMESPACE_PREDICATE*/"

// eventsSQLTemplate has one placeholder for the optional composition predicate.
// The row limit is bound via the {limit:UInt32} ClickHouse query parameter.
// (Single % in formatDateTime is correct: this string is NOT passed through
// fmt.Sprintf, unlike pollSQL.)
const eventsSQLTemplate = `SELECT
    toUnixTimestamp(Timestamp)                                                      AS ts_unix,
    ifNull(LogAttributes['krateo.io/composition-id'], '')                           AS composition_id,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'apiVersion'), '')   AS obj_apiversion,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'name'), '')         AS obj_name,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'namespace'), '')    AS obj_namespace,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'uid'), '')          AS obj_uid,
    ifNull(JSONExtractString(Body, 'object', 'involvedObject', 'kind'), '')         AS obj_kind,
    ifNull(JSONExtractString(Body, 'object', 'reason'), '')                         AS reason,
    ifNull(JSONExtractString(Body, 'object', 'message'), '')                        AS message,
    ifNull(JSONExtractString(Body, 'object', 'type'), 'Normal')                     AS type,
    coalesce(
        nullIf(JSONExtractString(Body, 'object', 'eventTime'), ''),
        nullIf(JSONExtractString(Body, 'object', 'lastTimestamp'), ''),
        formatDateTime(toDateTime(Timestamp), '%Y-%m-%dT%H:%i:%SZ', 'UTC')
    )                                                                                AS event_time,
    ifNull(JSONExtractString(Body, 'object', 'source', 'component'), '')            AS source_component
FROM otel_logs
WHERE ResourceAttributes['telemetry.source'] = 'k8s-events'
  AND JSONExtractString(Body, 'object', 'reason') != ''/*COMPOSITION_PREDICATE*//*NAMESPACE_PREDICATE*/
ORDER BY Timestamp DESC
LIMIT {limit:UInt32}
FORMAT JSONEachRow`

// compositionIDFilter is the predicate appended to the WHERE clause when a
// composition_id is supplied. It keys on the event's own involvedObject uid
// (the composition CR's uid) — NOT LogAttributes['krateo.io/composition-id'],
// which the collector resolves to the *owning* composition (and only for
// pod-associated objects), so it is absent/wrong for top-level compositions
// like user blueprints. The value is bound, not interpolated.
const compositionIDFilter = "\n  AND JSONExtractString(Body, 'object', 'involvedObject', 'uid') = {composition_id:String}"

// namespaceFilter is the predicate appended when the caller is RBAC-scoped to
// a set of namespaces (see rbac.go). The set is bound as a ClickHouse
// Array(String) parameter, never interpolated; every element is additionally
// validated as an RFC 1123 label before serialization (defence in depth).
// Cluster-scoped events (empty namespace) are excluded for scoped callers by
// construction — ” is never in the allowed set.
const namespaceFilter = "\n  AND JSONExtractString(Body, 'object', 'involvedObject', 'namespace') IN {namespaces:Array(String)}"

// uuidRe matches an RFC 4122 UUID (the shape of a krateo composition id),
// case-insensitively. Used to reject anything that is not a UUID before the
// value is bound as a ClickHouse parameter (defence in depth).
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// parseLimit parses the `limit` query param, returning a value in
// [1, defaultEventsLimit]. Empty/invalid/non-positive ⇒ defaultEventsLimit.
func parseLimit(raw string) int {
	if raw == "" {
		return defaultEventsLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultEventsLimit
	}
	if n > defaultEventsLimit {
		return defaultEventsLimit
	}
	return n
}

// buildEventsQuery returns the SQL and the bound ClickHouse parameters for the
// given request query values and RBAC scope (nil scope or allowAll ⇒ no
// namespace predicate). It returns an error when composition_id is present but
// not a valid UUID, or when a scoped namespace is not a valid RFC 1123 label
// (which cannot happen for names obtained from the K8s API — defence in
// depth). limit is always clamped to a safe range.
func buildEventsQuery(q url.Values, scope *nsScope) (string, map[string]string, error) {
	params := map[string]string{
		"limit": strconv.Itoa(parseLimit(q.Get("limit"))),
	}

	predicate := ""
	if cid := q.Get("composition_id"); cid != "" {
		if !uuidRe.MatchString(cid) {
			return "", nil, fmt.Errorf("composition_id must be a UUID")
		}
		predicate = compositionIDFilter
		params["composition_id"] = cid
	}

	nsPredicate := ""
	if scope.restricted() {
		arr, err := clickhouseArrayParam(scope.namespaces)
		if err != nil {
			return "", nil, err
		}
		nsPredicate = namespaceFilter
		params["namespaces"] = arr
	}

	query := strings.Replace(eventsSQLTemplate, compositionPredicatePlaceholder, predicate, 1)
	query = strings.Replace(query, namespacePredicatePlaceholder, nsPredicate, 1)
	return query, params, nil
}

// resolveRequestScope derives the caller's namespace scope from the verified
// identity in the request context. A nil scoper (scoping disabled) yields a
// nil scope (unrestricted). Errors mean the scope could NOT be established and
// the caller must fail closed (startup guarantees auth is enabled whenever the
// scoper is non-nil, so a missing identity here is a programming error, not a
// reachable state).
func resolveRequestScope(r *http.Request, scoper *rbacScoper) (*nsScope, error) {
	if scoper == nil {
		return nil, nil
	}
	ui, ok := userInfoFrom(r.Context())
	if !ok {
		return nil, fmt.Errorf("rbac scoping enabled but no verified identity on request")
	}
	return scoper.scopeFor(r.Context(), ui)
}

func handleEvents(cfg config, client *http.Client, scoper *rbacScoper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		scope, err := resolveRequestScope(r, scoper)
		if err != nil {
			// Fail closed: no scope ⇒ no data.
			slogErrorCtx(ctx, "events", "rbac scope resolution failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if scope.restricted() && len(scope.namespaces) == 0 {
			// Authenticated but authorized for no namespace: empty result,
			// no ClickHouse round-trip.
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			fmt.Fprint(w, "[]")
			return
		}

		query, params, err := buildEventsQuery(r.URL.Query(), scope)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		rows, err := queryClickHouse(ctx, client, cfg, query, params)
		if err != nil {
			slogErrorCtx(ctx, "events", "clickhouse query failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		events := make([]SSEK8sEvent, 0, len(rows))
		for _, row := range rows {
			events = append(events, row.toSSEK8sEvent())
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if err := json.NewEncoder(w).Encode(events); err != nil {
			slogErrorCtx(ctx, "events", "response encode failed", err)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

func handleSSE(h *hub, scoper *rbacScoper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		// Resolve the caller's RBAC namespace scope BEFORE upgrading to a
		// stream; fail closed when it cannot be established. The scope is
		// fixed for the lifetime of the connection (RBAC changes apply on
		// reconnect, within the scoper's cache TTL).
		scope, err := resolveRequestScope(r, scoper)
		if err != nil {
			slogErrorCtx(r.Context(), "sse", "rbac scope resolution failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

		// Optional per-composition subscription. When composition_id is present
		// the client receives ONLY that composition's events (the poller
		// broadcasts them under topic == composition_id). When absent the client
		// subscribes to the global firehose and filters client-side by event
		// name, the original behaviour.
		topic := r.URL.Query().Get("composition_id")

		c := &client{ch: make(chan sseMessage, 64), topic: topic, scope: scope}
		h.register(c)
		defer h.unregister(c)

		// Initial comment confirms the connection to the browser.
		fmt.Fprintf(w, ": connected\n\n")
		flusher.Flush()

		keepalive := time.NewTicker(25 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-keepalive.C:
				// SSE comment as a keepalive ping to prevent proxy timeouts.
				fmt.Fprintf(w, ": keepalive\n\n")
				flusher.Flush()
			case msg, ok := <-c.ch:
				if !ok {
					return
				}
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.topic, msg.data)
				flusher.Flush()
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	// Structured JSON logging as the process default (non-gated; parity with
	// authn/snowplow). Done first so every subsequent line is structured.
	initLogging()

	cfg := loadConfig()
	// Token verification is mandatory by design; a config that cannot resolve a
	// JWKS source is fatal (never fail-open to serving unauthenticated).
	auth, err := loadAuthConfig()
	if err != nil {
		slogError("sse-proxy", "auth configuration failed — token verification is mandatory", err)
		os.Exit(1)
	}
	auth.logStatus()

	// RBAC-derived multi-tenant scoping (see rbac.go). Misconfiguration is
	// fatal — the proxy must never start half-scoped (fail closed). Auth is
	// always enforced above, so the scoper always has a verified identity.
	scoper, err := loadRBACScoper()
	if err != nil {
		slogError("sse-proxy", "rbac scoping init failed", err)
		os.Exit(1)
	}
	scoper.logStatus()

	h := newHub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OTel setup is gated default-OFF (OTEL_TRACING_ENABLED / OTEL_METRICS_ENABLED).
	// When both are off this installs nothing and the wrappers below are skipped,
	// keeping the binary byte-identical in behaviour to the pre-OTel version.
	tel, shutdownTel := setupTelemetry(ctx)
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if err := shutdownTel(shutdownCtx); err != nil {
			slogError("telemetry", "shutdown error", err)
		}
	}()
	tel.logStatus()

	// Metrics instruments register on the global MeterProvider, which is a no-op
	// unless metrics are enabled — safe to init unconditionally.
	metrics, err := initMetrics(h)
	if err != nil {
		slogError("telemetry", "metric instrument init failed", err)
		metrics = nil
	}

	// Outbound HTTP client for the poller / events queries. When tracing is on,
	// wrap the transport with otelhttp so calls to ClickHouse are client-spanned
	// and propagate the W3C traceparent; otherwise use the default transport
	// unchanged.
	outboundClient := &http.Client{}
	if tel.tracingEnabled {
		outboundClient.Transport = otelhttp.NewTransport(http.DefaultTransport)
	}

	p := newPoller(cfg, h, metrics, outboundClient)
	go p.run(ctx)

	mux := http.NewServeMux()
	// /events takes the JWT in the Authorization header (RESTAction forwards it);
	// /notifications additionally accepts it via cookie/query param (EventSource
	// cannot set headers). /health stays unauthenticated for k8s probes.
	mux.HandleFunc("/events", auth.requireBearer(handleEvents(cfg, outboundClient, scoper)))
	mux.HandleFunc("/notifications/", auth.requireSSEToken(handleSSE(h, scoper)))
	mux.HandleFunc("/notifications", auth.requireSSEToken(handleSSE(h, scoper)))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// When tracing is on, wrap the whole mux with otelhttp so inbound requests
	// get a server span and the browser's traceparent is extracted into the
	// request context. /health is filtered out (noise). The SSE handler keeps
	// its own http.Flusher behaviour intact: otelhttp.NewHandler does not buffer
	// the response — it wraps the ResponseWriter while preserving the Flusher
	// interface — so the long-lived stream still flushes per message. The server
	// span for an SSE connection therefore spans the connection lifetime (it
	// ends when the client disconnects); the short-lived /events fetch gets a
	// normal request-scoped span.
	var handler http.Handler = mux
	if tel.tracingEnabled {
		handler = otelhttp.NewHandler(mux, "sse-proxy",
			otelhttp.WithFilter(func(r *http.Request) bool {
				return r.URL.Path != "/health"
			}),
		)
	}

	srv := &http.Server{
		Addr:    cfg.listenAddr,
		Handler: handler,
	}

	// Graceful shutdown on SIGTERM/SIGINT (Kubernetes sends SIGTERM on pod stop).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slogInfo("sse-proxy", "received signal, shutting down gracefully", slog.String("signal", sig.String()))
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slogError("sse-proxy", "http shutdown error", err)
		}
	}()

	slogInfo("sse-proxy", "listening", slog.String("addr", cfg.listenAddr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slogError("sse-proxy", "fatal server error", err)
		os.Exit(1)
	}
	slogInfo("sse-proxy", "stopped")
}
