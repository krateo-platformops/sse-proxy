package main

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// parseLimit
// ---------------------------------------------------------------------------

func TestParseLimit(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		{"empty defaults", "", defaultEventsLimit},
		{"valid within range", "50", 50},
		{"one", "1", 1},
		{"at cap", "200", 200},
		{"above cap clamps", "5000", defaultEventsLimit},
		{"zero defaults", "0", defaultEventsLimit},
		{"negative defaults", "-10", defaultEventsLimit},
		{"non-numeric defaults", "abc", defaultEventsLimit},
		{"injection attempt defaults", "10; DROP TABLE otel_logs", defaultEventsLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseLimit(tc.raw); got != tc.want {
				t.Fatalf("parseLimit(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// buildEventsQuery — composition_id filter + limit binding + injection safety
// ---------------------------------------------------------------------------

func mustValues(t *testing.T, raw string) url.Values {
	t.Helper()
	v, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", raw, err)
	}
	return v
}

func TestBuildEventsQuery_NoParams(t *testing.T) {
	q, params, err := buildEventsQuery(url.Values{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(q, "involvedObject', 'uid') = {composition_id") {
		t.Errorf("query should not contain composition predicate when unfiltered:\n%s", q)
	}
	if _, ok := params["composition_id"]; ok {
		t.Errorf("composition_id param should be absent when unfiltered, got %v", params)
	}
	if params["limit"] != "200" {
		t.Errorf("default limit param = %q, want \"200\"", params["limit"])
	}
	if !strings.Contains(q, "LIMIT {limit:UInt32}") {
		t.Errorf("query must bind limit as a parameter:\n%s", q)
	}
}

func TestBuildEventsQuery_WithValidComposition(t *testing.T) {
	cid := "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	q, params, err := buildEventsQuery(mustValues(t, "composition_id="+cid+"&limit=10"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q, "JSONExtractString(Body, 'object', 'involvedObject', 'uid') = {composition_id:String}") {
		t.Errorf("query must contain the bound composition predicate:\n%s", q)
	}
	if params["composition_id"] != cid {
		t.Errorf("composition_id param = %q, want %q", params["composition_id"], cid)
	}
	if params["limit"] != "10" {
		t.Errorf("limit param = %q, want \"10\"", params["limit"])
	}
	// The raw value must never appear inline in the SQL (only as a bound param).
	if strings.Contains(q, cid) {
		t.Errorf("composition_id value must NOT be interpolated into the SQL:\n%s", q)
	}
}

func TestBuildEventsQuery_RejectsNonUUIDCompositionID(t *testing.T) {
	bad := []string{
		"not-a-uuid",
		"1b4e28ba-2fa1-11d2-883f-0016d3cca427' OR '1'='1",
		"'; DROP TABLE otel_logs; --",
		"12345",
		"1b4e28ba2fa111d2883f0016d3cca427", // no dashes
	}
	for _, b := range bad {
		t.Run(b, func(t *testing.T) {
			v := url.Values{}
			v.Set("composition_id", b)
			if _, _, err := buildEventsQuery(v, nil); err == nil {
				t.Fatalf("expected error for invalid composition_id %q, got nil", b)
			}
		})
	}
}

func TestBuildEventsQuery_ClampsLimit(t *testing.T) {
	_, params, err := buildEventsQuery(mustValues(t, "limit=999999"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if params["limit"] != "200" {
		t.Errorf("limit should clamp to 200, got %q", params["limit"])
	}
}

// ---------------------------------------------------------------------------
// client topic filtering
// ---------------------------------------------------------------------------

func TestClientWants(t *testing.T) {
	cases := []struct {
		name       string
		subscribed string
		msgTopic   string
		want       bool
	}{
		{"global gets krateo", "", "krateo", true},
		{"global gets composition", "", "comp-123", true},
		{"composition gets own", "comp-123", "comp-123", true},
		{"composition ignores other", "comp-123", "comp-999", false},
		{"composition ignores global krateo", "comp-123", "krateo", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &client{topic: tc.subscribed}
			if got := c.wants(sseMessage{topic: tc.msgTopic}); got != tc.want {
				t.Fatalf("client{topic:%q}.wants(%q) = %v, want %v", tc.subscribed, tc.msgTopic, got, tc.want)
			}
		})
	}
}

// hub.broadcast only delivers to clients whose topic matches.
func TestHubBroadcastRespectsTopic(t *testing.T) {
	h := newHub()
	global := &client{ch: make(chan sseMessage, 4), topic: ""}
	compA := &client{ch: make(chan sseMessage, 4), topic: "A"}
	compB := &client{ch: make(chan sseMessage, 4), topic: "B"}
	h.register(global)
	h.register(compA)
	h.register(compB)

	h.broadcast(sseMessage{topic: "krateo", data: []byte("g")})
	h.broadcast(sseMessage{topic: "A", data: []byte("a")})

	// global: both messages.
	if got := len(global.ch); got != 2 {
		t.Errorf("global client received %d msgs, want 2", got)
	}
	// compA: only the "A" message.
	if got := len(compA.ch); got != 1 {
		t.Errorf("compA client received %d msgs, want 1", got)
	} else if msg := <-compA.ch; msg.topic != "A" {
		t.Errorf("compA received topic %q, want \"A\"", msg.topic)
	}
	// compB: nothing.
	if got := len(compB.ch); got != 0 {
		t.Errorf("compB client received %d msgs, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// toSSEK8sEvent maps composition_id through to the output
// ---------------------------------------------------------------------------

func TestToSSEK8sEventCarriesCompositionID(t *testing.T) {
	row := chRow{
		CompositionID: "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
		ObjName:       "foo",
		Reason:        "Created",
	}
	evt := row.toSSEK8sEvent()
	if evt.CompositionID != row.CompositionID {
		t.Errorf("CompositionID = %q, want %q", evt.CompositionID, row.CompositionID)
	}
}

// ---------------------------------------------------------------------------
// auth: token extraction
// ---------------------------------------------------------------------------

func TestBearerFromHeader(t *testing.T) {
	cases := []struct {
		name      string
		header    string
		wantTok   string
		wantFound bool
	}{
		{"valid bearer", "Bearer abc.def.ghi", "abc.def.ghi", true},
		{"case-insensitive scheme", "bearer abc", "abc", true},
		{"mixed case scheme", "BeArEr abc", "abc", true},
		{"missing", "", "", false},
		{"wrong scheme", "Basic abc", "", false},
		{"scheme only", "Bearer", "", false},
		{"empty token", "Bearer ", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodGet, "/events", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			tok, found := bearerFromHeader(r)
			if found != tc.wantFound || tok != tc.wantTok {
				t.Fatalf("bearerFromHeader(%q) = (%q,%v), want (%q,%v)", tc.header, tok, found, tc.wantTok, tc.wantFound)
			}
		})
	}
}

func TestTokenFromRequestSSE_Precedence(t *testing.T) {
	a := authConfig{signingKey: "k", sessionCookie: "krateo-session"}

	// Header wins over cookie and query.
	r, _ := http.NewRequest(http.MethodGet, "/notifications?access_token=qtok", nil)
	r.Header.Set("Authorization", "Bearer htok")
	r.AddCookie(&http.Cookie{Name: "krateo-session", Value: "ctok"})
	if tok, ok := a.tokenFromRequestSSE(r); !ok || tok != "htok" {
		t.Errorf("header should win: got (%q,%v)", tok, ok)
	}

	// Cookie wins over query when no header.
	r, _ = http.NewRequest(http.MethodGet, "/notifications?access_token=qtok", nil)
	r.AddCookie(&http.Cookie{Name: "krateo-session", Value: "ctok"})
	if tok, ok := a.tokenFromRequestSSE(r); !ok || tok != "ctok" {
		t.Errorf("cookie should win over query: got (%q,%v)", tok, ok)
	}

	// Query param fallback (access_token preferred over token).
	r, _ = http.NewRequest(http.MethodGet, "/notifications?token=t2&access_token=t1", nil)
	if tok, ok := a.tokenFromRequestSSE(r); !ok || tok != "t1" {
		t.Errorf("access_token should be preferred: got (%q,%v)", tok, ok)
	}

	// token used when access_token absent.
	r, _ = http.NewRequest(http.MethodGet, "/notifications?token=t2", nil)
	if tok, ok := a.tokenFromRequestSSE(r); !ok || tok != "t2" {
		t.Errorf("token fallback: got (%q,%v)", tok, ok)
	}

	// Nothing present.
	r, _ = http.NewRequest(http.MethodGet, "/notifications", nil)
	if tok, ok := a.tokenFromRequestSSE(r); ok || tok != "" {
		t.Errorf("expected no token, got (%q,%v)", tok, ok)
	}
}

func TestRequireBearer_DisabledIsPassthrough(t *testing.T) {
	a := authConfig{} // no signing key => disabled
	if a.enabled() {
		t.Fatal("auth should be disabled with empty signing key")
	}
	called := false
	h := a.requireBearer(func(http.ResponseWriter, *http.Request) { called = true })
	r, _ := http.NewRequest(http.MethodGet, "/events", nil)
	h(nil, r)
	if !called {
		t.Error("disabled auth must pass through to the wrapped handler")
	}
}
