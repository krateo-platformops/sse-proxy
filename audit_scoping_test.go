package main

import (
	"testing"
	"time"
)

// eventsScoper is a stand-in for a loaded events scoper. It never calls the API: every assertion
// here is about CONFIGURATION RESOLUTION, so a kube client would add nothing but coupling.
//
// ITS VALUES ARE DELIBERATELY NOT THE BUILT-IN DEFAULTS, and that is what gives the inheritance
// tests teeth. My first version used verb "list" / resource "events" -- the same strings as
// defaultScopingVerb / defaultScopingResource -- so "inherit from the events scoper" and "reset to
// the built-in default" produced identical results and the partial-config test passed either way.
// Mutation-testing it caught that: injecting `verb = defaultScopingVerb` into the inheritance
// branch left the suite green. With non-default values here, that mutant fails.
func eventsScoper() *rbacScoper {
	return &rbacScoper{
		verb:     "watch",
		apiGroup: "tenancy.krateo.io",
		resource: "workspaces",
		ttl:      30 * time.Second,
		cache:    map[string]scopeEntry{},
	}
}

// THE INERTNESS CLAIM. With no AUDIT_SCOPING_* set, /audit must get the SAME POINTER as /events --
// not an equal copy. A copy would behave identically today and diverge the moment either side
// gained a field, and it would double the SubjectAccessReview traffic for no reason.
func TestLoadAuditScoper_UnconfiguredReturnsTheEventsScoperItself(t *testing.T) {
	for _, k := range []string{envAuditScopingVerb, envAuditScopingResource, envAuditScopingAPIGroup} {
		t.Setenv(k, "")
	}
	ev := eventsScoper()
	got := loadAuditScoper(ev)
	if got != ev {
		t.Fatalf("expected the events scoper itself, got a different instance %+v", got)
	}
}

// Scoping off means nothing to scope. /audit must not invent an enforcement the rest of the proxy
// is not applying.
func TestLoadAuditScoper_NilEventsScoperYieldsNil(t *testing.T) {
	t.Setenv(envAuditScopingVerb, "get")
	t.Setenv(envAuditScopingResource, "auditrecords")
	if got := loadAuditScoper(nil); got != nil {
		t.Fatalf("expected nil when events scoping is disabled, got %+v", got)
	}
}

func TestLoadAuditScoper_FullyConfiguredDiverges(t *testing.T) {
	t.Setenv(envAuditScopingVerb, "get")
	t.Setenv(envAuditScopingResource, "auditrecords")
	t.Setenv(envAuditScopingAPIGroup, "audit.krateo.io")
	ev := eventsScoper()
	got := loadAuditScoper(ev)
	if got == ev {
		t.Fatal("expected a distinct scoper once configured")
	}
	if got.verb != "get" || got.resource != "auditrecords" || got.apiGroup != "audit.krateo.io" {
		t.Fatalf("wrong SAR: verb=%q resource=%q apiGroup=%q", got.verb, got.resource, got.apiGroup)
	}
	// shared deliberately
	if got.ttl != ev.ttl {
		t.Fatalf("ttl should be inherited: got %v want %v", got.ttl, ev.ttl)
	}
	// CONTROL: the events scoper must be untouched. A constructor that mutated its input would
	// change what /events enforces as a side effect of configuring /audit.
	if ev.verb != "watch" || ev.resource != "workspaces" || ev.apiGroup != "tenancy.krateo.io" {
		t.Fatalf("events scoper was mutated: %+v", ev)
	}
}

// THE CACHE IS THE AUTHORIZATION BUG. cacheKey is username+groups only -- it does not include the
// verb, resource or apiGroup. If the two scopers shared one map, two DIFFERENT SubjectAccessReviews
// about the SAME caller would collide on one entry and whichever resolved first would answer for
// both: an events permission served as an audit scope, or the reverse. Silent, and it would look
// like correct caching.
func TestLoadAuditScoper_DivergedScoperDoesNotShareTheCache(t *testing.T) {
	t.Setenv(envAuditScopingVerb, "get")
	t.Setenv(envAuditScopingResource, "auditrecords")
	ev := eventsScoper()
	au := loadAuditScoper(ev)

	const key = "alice\x00devs"
	ev.cache[key] = scopeEntry{
		scope:   &nsScope{namespaces: []string{"tenant-a"}},
		expires: time.Now().Add(time.Hour),
	}
	if _, leaked := au.cache[key]; leaked {
		t.Fatal("the audit scoper can see the events scoper's cache entry — one SAR's answer would serve the other")
	}
	// and the reverse direction
	au.cache[key] = scopeEntry{scope: &nsScope{allowAll: true}, expires: time.Now().Add(time.Hour)}
	if got := ev.cache[key].scope; got == nil || got.allowAll {
		t.Fatalf("the events cache entry was overwritten by the audit scoper: %+v", got)
	}
}

// A PARTIAL configuration must inherit, not reset. Setting only the resource should change the
// resource; silently resetting the verb to a built-in default would be a surprise an operator
// would discover as an authorization change they did not ask for.
func TestLoadAuditScoper_PartialConfigInheritsFromEvents(t *testing.T) {
	cases := []struct {
		name                         string
		verb, resource, apiGroup     string
		wantVerb, wantRes, wantGroup string
	}{
		{"resource only", "", "auditrecords", "", "watch", "auditrecords", "tenancy.krateo.io"},
		{"verb only", "get", "", "", "get", "workspaces", "tenancy.krateo.io"},
		{"apiGroup only", "", "", "audit.krateo.io", "watch", "workspaces", "audit.krateo.io"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envAuditScopingVerb, c.verb)
			t.Setenv(envAuditScopingResource, c.resource)
			t.Setenv(envAuditScopingAPIGroup, c.apiGroup)
			got := loadAuditScoper(eventsScoper())
			if got.verb != c.wantVerb || got.resource != c.wantRes || got.apiGroup != c.wantGroup {
				t.Fatalf("verb=%q resource=%q apiGroup=%q; want %q/%q/%q",
					got.verb, got.resource, got.apiGroup, c.wantVerb, c.wantRes, c.wantGroup)
			}
		})
	}
}
