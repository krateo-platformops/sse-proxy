package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestBuildAuditQuery_Unscoped(t *testing.T) {
	q, params, err := buildAuditQuery(url.Values{}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "IN {namespaces:Array(String)}") {
		t.Fatalf("unscoped query must carry no namespace predicate:\n%s", q)
	}
	if _, ok := params["namespaces"]; ok {
		t.Fatalf("unscoped query must bind no namespaces param")
	}
	if params["limit"] != "200" {
		t.Fatalf("default limit = %q, want 200", params["limit"])
	}
	// The placeholders must be fully substituted (no literal markers left).
	if strings.Contains(q, "/*NAMESPACE_PREDICATE*/") || strings.Contains(q, "/*WINDOW_PREDICATE*/") {
		t.Fatalf("unsubstituted placeholder left in query:\n%s", q)
	}
	// event.name discriminator is always present.
	if !strings.Contains(q, "LogAttributes['event.name'] = 'audit'") {
		t.Fatalf("query missing the audit discriminator:\n%s", q)
	}
}

func TestBuildAuditQuery_ScopedInjectsBoundPredicate(t *testing.T) {
	scope := &nsScope{allowAll: false, namespaces: []string{"tenant-a", "tenant-b"}}
	q, params, err := buildAuditQuery(url.Values{}, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "LogAttributes['k8s.namespace.name'] IN {namespaces:Array(String)}") {
		t.Fatalf("scoped query must carry the bound namespace predicate:\n%s", q)
	}
	if params["namespaces"] != "['tenant-a','tenant-b']" {
		t.Fatalf("namespaces param = %q, want ['tenant-a','tenant-b']", params["namespaces"])
	}
	// The namespace values must never be interpolated into the SQL text itself.
	if strings.Contains(q, "tenant-a") || strings.Contains(q, "tenant-b") {
		t.Fatalf("namespace values must be bound as a param, not interpolated:\n%s", q)
	}
}

func TestBuildAuditQuery_LimitClampedAndBound(t *testing.T) {
	cases := map[string]string{"": "200", "50": "50", "99999": "200", "-1": "200", "abc": "200", "0": "200"}
	for in, want := range cases {
		_, params, err := buildAuditQuery(url.Values{"limit": {in}}, nil, 0)
		if err != nil {
			t.Fatalf("limit=%q: %v", in, err)
		}
		if params["limit"] != want {
			t.Errorf("limit=%q -> %q, want %q", in, params["limit"], want)
		}
	}
	// limit is bound as a typed ClickHouse parameter, never concatenated.
	q, _, _ := buildAuditQuery(url.Values{}, nil, 0)
	if !strings.Contains(q, "LIMIT {limit:UInt32}") {
		t.Fatalf("limit must be bound as {limit:UInt32}:\n%s", q)
	}
}

func TestBuildAuditQuery_WindowBound(t *testing.T) {
	q, params, err := buildAuditQuery(url.Values{}, nil, 1791960000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "Timestamp >= toDateTime64({since:Int64}, 9)") {
		t.Fatalf("windowed query must bind {since:Int64}:\n%s", q)
	}
	if params["since"] != "1791960000" {
		t.Fatalf("since param = %q", params["since"])
	}
}

func TestQueryAuditRecords_ParsesAndBindsParams(t *testing.T) {
	var gotParamLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotParamLimit = r.URL.Query().Get("param_limit")
		// two JSONEachRow lines
		_, _ = w.Write([]byte(
			`{"timestamp":"2026-10-28T10:00:00Z","user":"mario.rossi","action":"call","verb":"POST","namespace":"tenant-a","outcome":"success","statusCode":"201","traceId":"abc","sessionId":"s1"}` + "\n" +
				`{"timestamp":"2026-10-28T10:01:00Z","user":"anna","action":"call","verb":"DELETE","namespace":"tenant-a","outcome":"failure","statusCode":"403"}` + "\n"))
	}))
	defer srv.Close()

	cfg := config{clickhouseURL: srv.URL}
	query, params, err := buildAuditQuery(url.Values{"limit": {"50"}}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := queryAuditRecords(context.Background(), srv.Client(), cfg, query, params)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].User != "mario.rossi" || rows[0].Outcome != "success" || rows[0].StatusCode != "201" {
		t.Fatalf("row0 mismatch: %+v", rows[0])
	}
	if rows[1].Verb != "DELETE" || rows[1].Outcome != "failure" {
		t.Fatalf("row1 mismatch: %+v", rows[1])
	}
	if gotParamLimit != "50" {
		t.Fatalf("param_limit sent to ClickHouse = %q, want 50", gotParamLimit)
	}
}
