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
//
// EACH ALIAS IS THE JSON KEY OF AN AuditRecord FIELD, spelled exactly: the
// JSONEachRow row is decoded straight into AuditRecord, and encoding/json
// ignores case but not underscores. Until 1.5.0 six aliases were snake_case
// (resource_group, resource_version, resource_name, status_code, trace_id,
// session_id), so those fields were always empty in /audit responses.
// TestAuditSQLAliasesMatchRecordKeys keeps the two in step.
const auditSQLTemplate = `SELECT
    formatDateTime(toDateTime(Timestamp), '%Y-%m-%dT%H:%i:%SZ', 'UTC')        AS timestamp,
    ifNull(nullIf(LogAttributes['user.name'], ''), LogAttributes['enduser.id']) AS user,
    ifNull(LogAttributes['user.roles'], '')                                   AS roles,
    ifNull(LogAttributes['krateo.action'], '')                                AS action,
    ifNull(LogAttributes['http.request.method'], '')                          AS verb,
    ifNull(LogAttributes['k8s.resource.group'], '')                           AS resourceGroup,
    ifNull(LogAttributes['k8s.resource.version'], '')                         AS resourceVersion,
    ifNull(LogAttributes['k8s.resource.resource'], '')                        AS resource,
    ifNull(LogAttributes['k8s.resource.name'], '')                            AS resourceName,
    ifNull(LogAttributes['k8s.namespace.name'], '')                           AS namespace,
    ifNull(LogAttributes['outcome'], '')                                      AS outcome,
    ifNull(LogAttributes['http.response.status_code'], '')                    AS statusCode,
    Body                                                                      AS message,
    TraceId                                                                   AS traceId,
    ifNull(LogAttributes['session.id'], '')                                   AS sessionId
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
//
// It goes through postClickHouse, the round trip /events uses, so an audit read
// carries the same server-side settings: readonly=2,
// cancel_http_readonly_queries_on_client_close=1 and, under handleAudit's
// deadline, max_execution_time. Before, it had its own round trip with none of
// them, so an /audit read abandoned by its caller ran to completion on
// ClickHouse, the defect /events had (see queryClickHouse).
func queryAuditRecords(ctx context.Context, client *http.Client, cfg config, query string, params map[string]string) ([]AuditRecord, error) {
	body, err := postClickHouse(ctx, client, cfg, query, params)
	if err != nil {
		return nil, err
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

// auditScopingDisabledBody is the refusal /audit returns when namespace scoping is off. It names
// the cause and the fix, because a bare status code sends the reader looking at their own token.
const auditScopingDisabledBody = `{"error":"audit is unavailable: namespace scoping is disabled",` +
	`"reason":"RBACScopingDisabled",` +
	`"detail":"/audit cannot enforce tenant isolation unless RBAC_SCOPING_ENABLED=true. ` +
	`Refusing rather than returning every namespace's audit records."}`

func handleAudit(cfg config, client *http.Client, scoper *rbacScoper) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// FAIL CLOSED WHEN SCOPING IS OFF, IN THE BINARY. A nil scoper means scoping is disabled,
		// and resolveRequestScope then yields a nil *nsScope -- which nsScope.allows and
		// buildAuditQuery both treat as allowAll, adding NO namespace predicate. That is the right
		// behaviour for /events (historic, low sensitivity) and the wrong one for the audit trail:
		// it would serve every namespace's records, plus the cluster-scoped ones, to any caller
		// holding a valid bearer token.
		//
		// WHY HERE AND NOT AS A DEPLOYMENT DEFAULT. A chart default protects only the install paths
		// that use our values. `features.*` demonstrated the failure mode on the installer side the
		// same week: a value silently inert on an upgrade, so the safe setting was simply absent and
		// nothing said so. A refusal compiled into the binary cannot be dropped by a values file, a
		// hand-written manifest, or an operator who copied an older deployment.
		//
		// 503, NOT 403. The caller's token is not the problem and 403 would send them to check their
		// own permissions. The server is in a configuration where it cannot answer safely, which is
		// what 503 says. The route stays registered deliberately: a 404 would be fail-closed too but
		// leaves whoever hits it with no way to tell "not built" from "not configured".
		if scoper == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, auditScopingDisabledBody)
			return
		}

		// One deadline for the whole request, scope resolution included, as
		// for /events: a slow resolution must not get a full query budget on
		// top of it. A deadline passing during resolution is a 500, like a
		// deadline during the query, since /audit has no partial answer.
		rctx, rcancel := context.WithTimeout(ctx, cfg.auditRequestTimeout())
		defer rcancel()

		scope, err := resolveRequestScope(r.WithContext(rctx), scoper)
		if err != nil {
			if callerGone(r) {
				answerCallerGone(w, r, "audit", err)
				return
			}
			// Fail closed: no scope ⇒ no data.
			slogErrorCtx(ctx, "audit", "rbac scope resolution failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
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

		// One deadline for the query, below the caller's upstream timeout.
		// postClickHouse turns it into ClickHouse's max_execution_time, so
		// ClickHouse stops when this handler stops waiting. Unlike /events
		// there is no narrower answer to fall back to, so a deadline is
		// still a 500, as any failure was before. Derived from the request
		// deadline, so it is AUDIT_QUERY_TIMEOUT or what is left of
		// AUDIT_REQUEST_TIMEOUT, whichever ends first.
		qctx, cancel := context.WithTimeout(rctx, cfg.auditQueryTimeout())
		defer cancel()
		rows, err := queryAuditRecords(qctx, client, cfg, query, params)
		if err != nil {
			if callerGone(r) {
				answerCallerGone(w, r, "audit", err)
				return
			}
			slogErrorCtx(ctx, "audit", "clickhouse query failed", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		if err := json.NewEncoder(w).Encode(rows); err != nil {
			slogErrorCtx(ctx, "audit", "response encode failed", err)
		}
	}
}
