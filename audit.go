package main

// REST /audit — returns recent Krateo audit records as a plain JSON array,
// scoped to the caller's namespaces by the same fail-closed RBAC mechanism as
// /events (rbac.go). It reads the audit trail the OTel collector writes to
// ClickHouse `krateo_audit` (event.name="audit", 365-day retention), NOT
// otel_logs. snowplow deliberately does not host this read surface, so the
// portal's audit view calls here (C8 / krateo-blueprints/krateo-acmp#59).
//
// Tenant isolation (the C7 invariant, enforced here): a scoped caller only ever
// sees audit rows whose k8s.namespace.name is in its SubjectAccessReview-derived
// namespace set; cluster-scoped/admin audit rows (empty namespace) are visible
// only to an unscoped/allowAll caller. Writes are never reachable from here.
//
// SQL-injection safety: identical discipline to /events — nothing from the
// request is concatenated into SQL. `limit` is parsed to a bounded integer and
// bound as {limit:UInt32}; the namespace set is validated per element (RFC 1123)
// and bound as {namespaces:Array(String)}; `since` is bound as {since:Int64}.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// defaultAuditLimit is both the default and the hard cap for /audit.
const defaultAuditLimit = 200

// auditNamespacePredicatePlaceholder / auditWindowPredicatePlaceholder are
// replaced (via strings.Replace, never fmt.Sprintf — the template carries
// literal ClickHouse % directives) with the optional predicates.
const (
	auditNamespacePredicatePlaceholder = "/*NAMESPACE_PREDICATE*/"
	auditWindowPredicatePlaceholder    = "/*WINDOW_PREDICATE*/"
)

// auditSQLTemplate reads krateo_audit (the OTel logs schema the collector's
// clickhouse/audit exporter writes). The audit semconv attributes live in the
// LogAttributes map; TraceId/Timestamp are columns. Newest-first, limit bound.
const auditSQLTemplate = `SELECT
    formatDateTime(toDateTime(Timestamp), '%Y-%m-%dT%H:%i:%SZ', 'UTC')        AS timestamp,
    ifNull(nullIf(LogAttributes['user.name'], ''), LogAttributes['enduser.id']) AS user,
    ifNull(LogAttributes['user.roles'], '')                                   AS roles,
    ifNull(LogAttributes['krateo.action'], '')                                AS action,
    ifNull(LogAttributes['http.request.method'], '')                          AS verb,
    ifNull(LogAttributes['k8s.resource.group'], '')                           AS resource_group,
    ifNull(LogAttributes['k8s.resource.version'], '')                         AS resource_version,
    ifNull(LogAttributes['k8s.resource.resource'], '')                        AS resource,
    ifNull(LogAttributes['k8s.resource.name'], '')                            AS resource_name,
    ifNull(LogAttributes['k8s.namespace.name'], '')                           AS namespace,
    ifNull(LogAttributes['outcome'], '')                                      AS outcome,
    ifNull(LogAttributes['http.response.status_code'], '')                    AS status_code,
    Body                                                                      AS message,
    TraceId                                                                   AS trace_id,
    ifNull(LogAttributes['session.id'], '')                                   AS session_id
FROM krateo_audit
WHERE LogAttributes['event.name'] = 'audit'/*NAMESPACE_PREDICATE*//*WINDOW_PREDICATE*/
ORDER BY Timestamp DESC
LIMIT {limit:UInt32}
FORMAT JSONEachRow`

// auditNamespaceFilter scopes to the caller's namespaces (bound Array(String)).
// A scoped caller never sees cluster-scoped audit rows: the empty namespace is
// never in the set.
const auditNamespaceFilter = "\n  AND LogAttributes['k8s.namespace.name'] IN {namespaces:Array(String)}"

// auditWindowFilter bounds the query to rows at or after {since} (unix seconds),
// comparing the bare Timestamp column so the primary key prunes.
const auditWindowFilter = "\n  AND Timestamp >= toDateTime64({since:Int64}, 9)"

// AuditRecord is one row of the audit trail as returned to the portal.
type AuditRecord struct {
	Timestamp       string `json:"timestamp"`
	User            string `json:"user"`
	Roles           string `json:"roles,omitempty"`
	Action          string `json:"action"`
	Verb            string `json:"verb,omitempty"`
	ResourceGroup   string `json:"resourceGroup,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	Resource        string `json:"resource,omitempty"`
	ResourceName    string `json:"resourceName,omitempty"`
	Namespace       string `json:"namespace"`
	Outcome         string `json:"outcome"`
	StatusCode      string `json:"statusCode,omitempty"`
	Message         string `json:"message,omitempty"`
	TraceID         string `json:"traceId,omitempty"`
	SessionID       string `json:"sessionId,omitempty"`
}

// parseAuditLimit mirrors parseLimit, bounded to [1, defaultAuditLimit].
func parseAuditLimit(raw string) int {
	if raw == "" {
		return defaultAuditLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n > defaultAuditLimit {
		return defaultAuditLimit
	}
	return n
}

// buildAuditQuery returns the SQL and bound parameters for the given request
// values and RBAC scope (nil / allowAll ⇒ no namespace predicate). `since` ≤ 0
// means unbounded.
func buildAuditQuery(q url.Values, scope *nsScope, since int64) (string, map[string]string, error) {
	params := map[string]string{
		"limit": strconv.Itoa(parseAuditLimit(q.Get("limit"))),
	}

	nsPredicate := ""
	if scope.restricted() {
		arr, err := clickhouseArrayParam(scope.namespaces)
		if err != nil {
			return "", nil, err
		}
		nsPredicate = auditNamespaceFilter
		params["namespaces"] = arr
	}

	windowPredicate := ""
	if since > 0 {
		windowPredicate = auditWindowFilter
		params["since"] = strconv.FormatInt(since, 10)
	}

	query := strings.Replace(auditSQLTemplate, auditNamespacePredicatePlaceholder, nsPredicate, 1)
	query = strings.Replace(query, auditWindowPredicatePlaceholder, windowPredicate, 1)
	return query, params, nil
}

// queryAuditRecords runs the audit query and parses the JSONEachRow response.
// The ClickHouse round-trip mirrors queryClickHouse (same endpoint, param
// binding and basic-auth); kept separate so the /events path is untouched.
func queryAuditRecords(ctx context.Context, client *http.Client, cfg config, query string, params map[string]string) ([]AuditRecord, error) {
	if client == nil {
		client = http.DefaultClient
	}
	endpoint := cfg.clickhouseURL
	if len(params) > 0 {
		u, err := url.Parse(cfg.clickhouseURL)
		if err != nil {
			return nil, fmt.Errorf("parse clickhouse url: %w", err)
		}
		qp := u.Query()
		for k, v := range params {
			qp.Set("param_"+k, v)
		}
		u.RawQuery = qp.Encode()
		endpoint = u.String()
	}

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

	rows := make([]AuditRecord, 0)
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var row AuditRecord
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			slogErrorCtx(ctx, "audit", "row unmarshal failed", err, slog.String("line", line))
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func handleAudit(cfg config, client *http.Client, scoper *rbacScoper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		scope, err := resolveRequestScope(r, scoper)
		if err != nil {
			// Fail closed: no scope ⇒ no data.
			slogErrorCtx(ctx, "audit", "rbac scope resolution failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if v, ok := scope.scopeHeaderValue(); ok {
			w.Header().Set(scopeHeaderName, v)
		}

		if scope.restricted() && len(scope.namespaces) == 0 {
			// Authenticated but authorized for no namespace: empty, no round-trip.
			fmt.Fprint(w, "[]")
			return
		}

		query, params, err := buildAuditQuery(r.URL.Query(), scope, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		rows, err := queryAuditRecords(ctx, client, cfg, query, params)
		if err != nil {
			slogErrorCtx(ctx, "audit", "clickhouse query failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		if err := json.NewEncoder(w).Encode(rows); err != nil {
			slogErrorCtx(ctx, "audit", "response encode failed", err)
		}
	}
}
