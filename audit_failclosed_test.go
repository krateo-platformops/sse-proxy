package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// THE EXPOSURE THIS CLOSES. With scoping disabled the scoper is nil, resolveRequestScope yields a
// nil *nsScope, and both nsScope.allows and buildAuditQuery treat nil as allowAll -- no namespace
// predicate at all. For /events that is the historic, intended behaviour. For the audit trail it
// would serve every namespace's records, plus the cluster-scoped ones, to any caller with a valid
// bearer token. /audit must refuse instead.
func TestHandleAudit_RefusesWhenScopingDisabled(t *testing.T) {
	h := handleAudit(config{}, http.DefaultClient, nil) // nil scoper == scoping disabled

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/audit", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d — a nil scoper must NOT reach the query path",
			rr.Code, http.StatusServiceUnavailable)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	// The body must name the cause machine-readably. A bare status sends the reader to check their
	// own token, which is the one thing that is not wrong here.
	var body struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not valid JSON: %v (%q)", err, rr.Body.String())
	}
	if body.Reason != "RBACScopingDisabled" {
		t.Errorf("reason = %q, want RBACScopingDisabled", body.Reason)
	}
	if !strings.Contains(body.Detail, "RBAC_SCOPING_ENABLED") {
		t.Errorf("detail must name the variable that fixes it, got %q", body.Detail)
	}

	// IT MUST NOT HAVE RETURNED ROWS. An empty JSON array would also be "safe" in the sense of
	// leaking nothing, but it would be indistinguishable from "you are authorized for no
	// namespaces" -- and an operator would read it as working.
	if strings.TrimSpace(rr.Body.String()) == "[]" {
		t.Error("returned an empty array instead of refusing: that reads as a working endpoint")
	}
}

// THE CONTROL. With a scoper present the refusal must NOT fire -- otherwise this change would
// disable /audit everywhere and the test above would pass for the wrong reason.
func TestHandleAudit_DoesNotRefuseWhenScopingEnabled(t *testing.T) {
	sc := &rbacScoper{
		verb:     "get",
		resource: "auditrecords",
		apiGroup: "audit.krateo.io",
		cache:    map[string]scopeEntry{},
	}
	h := handleAudit(config{}, http.DefaultClient, sc)

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/audit", nil))

	if rr.Code == http.StatusServiceUnavailable {
		t.Fatalf("refused with 503 despite scoping being ENABLED — the guard is keying on the wrong thing")
	}
	if strings.Contains(rr.Body.String(), "RBACScopingDisabled") {
		t.Errorf("emitted the scoping-disabled body with a scoper present: %q", rr.Body.String())
	}
	// It will fail later for want of an identity in the context / a reachable ClickHouse; that is
	// not this test's business. All that matters is that it got PAST the disabled guard.
}
