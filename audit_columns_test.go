package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The /audit SELECT's column aliases ARE the JSON keys AuditRecord decodes.
// Before 1.5.0 six aliases were snake_case (resource_group, resource_version,
// resource_name, status_code, trace_id, session_id) while the struct tags are
// camelCase, so encoding/json (which ignores case but not underscores) never
// matched them and those six fields were always empty in /audit responses.

var auditAliasRe = regexp.MustCompile(`\bAS\s+([A-Za-z_][A-Za-z0-9_]*)`)

func auditSQLAliases() []string {
	sel := auditSQLTemplate[:strings.Index(auditSQLTemplate, "FROM krateo_audit")]
	var out []string
	for _, m := range auditAliasRe.FindAllStringSubmatch(sel, -1) {
		out = append(out, m[1])
	}
	sort.Strings(out)
	return out
}

func auditRecordKeys() []string {
	var out []string
	rt := reflect.TypeOf(AuditRecord{})
	for i := 0; i < rt.NumField(); i++ {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TestAuditSQLAliasesMatchRecordKeys: one alias per AuditRecord field, spelled
// exactly as its JSON key. Fails on the pre-1.5.0 template (six snake_case
// aliases).
func TestAuditSQLAliasesMatchRecordKeys(t *testing.T) {
	aliases, keys := auditSQLAliases(), auditRecordKeys()
	if !reflect.DeepEqual(aliases, keys) {
		t.Fatalf("audit SELECT aliases and AuditRecord JSON keys differ:\n aliases %v\n keys    %v", aliases, keys)
	}
}

// TestQueryAuditRecords_EveryColumnReachesTheResponse decodes a row keyed by
// the template's own aliases, as ClickHouse's JSONEachRow would return it, and
// requires every AuditRecord field to come through. Fails on the pre-1.5.0
// template: ResourceGroup, ResourceVersion, ResourceName, StatusCode, TraceID
// and SessionID stay empty.
func TestQueryAuditRecords_EveryColumnReachesTheResponse(t *testing.T) {
	row := map[string]string{}
	for _, a := range auditSQLAliases() {
		row[a] = "v-" + a
	}
	line, _ := json.Marshal(row)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write(append(line, '\n'))
	}))
	defer srv.Close()

	query, params, err := buildAuditQuery(url.Values{}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := queryAuditRecords(context.Background(), srv.Client(), config{clickhouseURL: srv.URL}, query, params)
	if err != nil || len(recs) != 1 {
		t.Fatalf("rows=%d err=%v", len(recs), err)
	}
	rv, rt := reflect.ValueOf(recs[0]), reflect.TypeOf(recs[0])
	for i := 0; i < rt.NumField(); i++ {
		if rv.Field(i).String() == "" {
			name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
			t.Errorf("AuditRecord.%s (json %q) is empty: no SELECT column decodes into it", rt.Field(i).Name, name)
		}
	}
}
