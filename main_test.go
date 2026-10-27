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
	if !strings.Contains(q, "LogAttributes['krateo.io/composition-id'] = {composition_id:String}") {
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
	a := testAuthConfig() // sessionCookie defaults to "krateo-session"

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

// ---------------------------------------------------------------------------
// buildEventsQuery — a composition's feed is its OWN events UNION its CHILDREN's
//
// Two mechanisms, two predicates: the composition CR's reconcile events carry
// involvedObject.uid and no label; its managed resources carry the
// krateo.io/composition-id label the composition-dynamic-controller stamps on
// them. Measured near-disjoint (4,331 vs 827 rows, overlapping on 1), so
// dropping either predicate silently halves the feed.
// ---------------------------------------------------------------------------

// TestBuildEventsQuery_CompositionIncludesOwnAndChildren — one attribute now
// carries BOTH halves of a composition's feed, so one predicate suffices.
// Children get krateo.io/composition-id from the controller's label; the
// composition's OWN reconcile events get it because otel-collector#11 returns a
// root composition's own metadata.uid as the composition-id. Measured on
// krateo-057: 1,612 of devday-milan's 1,613 own events carried the label.
// If that regresses, this endpoint silently serves half a feed.
func TestBuildEventsQuery_CompositionIncludesOwnAndChildren(t *testing.T) {
	cid := "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	q, params, err := buildEventsQuery(mustValues(t, "composition_id="+cid), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q, "LogAttributes['krateo.io/composition-id'] = {composition_id:String}") {
		t.Errorf("query must match on the composition-id label, which covers own events AND children:\n%s", q)
	}
	// One bound parameter; the raw value is never inlined.
	if params["composition_id"] != cid {
		t.Errorf("composition_id param = %q, want %q", params["composition_id"], cid)
	}
	if strings.Contains(q, cid) {
		t.Errorf("composition_id must NOT be interpolated into the SQL:\n%s", q)
	}
}

// TestBuildEventsQuery_ScopeCannotLeakThroughPrecedence guards an RBAC leak.
// The composition predicate is appended alongside the namespace predicate, which
// is AND'd. While the composition predicate was an OR'd pair it NEEDED its own
// parentheses — without them the OR binds looser than the AND and a scoped caller
// receives events from namespaces they cannot read. It is now a single equality,
// so there is no OR to bind loosely and the hazard is gone by construction. This
// test keeps that property pinned: if anyone reintroduces a top-level OR into the
// composition predicate, it must be parenthesised.
func TestBuildEventsQuery_ScopeCannotLeakThroughPrecedence(t *testing.T) {
	cid := "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	scope := &nsScope{namespaces: []string{"team-a"}}
	q, _, err := buildEventsQuery(mustValues(t, "composition_id="+cid), scope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if open, closed := strings.Count(q, "("), strings.Count(q, ")"); open != closed {
		t.Errorf("unbalanced parentheses (%d open, %d close):\n%s", open, closed, q)
	}
	if !strings.Contains(q, "{namespaces:Array(String)}") {
		t.Errorf("the namespace predicate must still be applied:\n%s", q)
	}
	// Isolate the appended predicates and reject any UNPARENTHESISED top-level OR.
	marker := "JSONExtractString(Body, 'object', 'reason') != ''"
	tail := q[strings.Index(q, marker)+len(marker):]
	for _, line := range strings.Split(tail, "\n") {
		t2 := strings.TrimSpace(line)
		if !strings.HasPrefix(t2, "AND ") {
			continue
		}
		clause := strings.TrimPrefix(t2, "AND ")
		if strings.Contains(clause, " OR ") && !strings.HasPrefix(clause, "(") {
			t.Errorf("top-level OR in an AND'd clause must be parenthesised or the namespace "+
				"scope leaks: %q", clause)
		}
	}
}

// ---------------------------------------------------------------------------
// buildEventsQuery — the composition predicate is ONE indexed map lookup
// ---------------------------------------------------------------------------

// TestBuildEventsQuery_CompositionPredicateIsMapOnly pins the property the whole
// fix rests on: NOTHING in the composition predicate may touch Body. Any
// JSONExtractString or hasToken over Body is a computed expression on a
// non-indexed column, which cannot prune and restores the ~2s-regardless-of-
// result-size behaviour this replaced (measured 18.49s for a composition with no
// events, against 0.42s map-only).
func TestBuildEventsQuery_CompositionPredicateIsMapOnly(t *testing.T) {
	cid := "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	q, params, err := buildEventsQuery(mustValues(t, "composition_id="+cid), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(q, "LogAttributes['krateo.io/composition-id'] = {composition_id:String}") {
		t.Errorf("composition predicate must be the indexed map lookup:\n%s", q)
	}
	// Extract just the appended predicate: everything after the sanity clause.
	marker := "JSONExtractString(Body, 'object', 'reason') != ''"
	idx := strings.Index(q, marker)
	if idx < 0 {
		t.Fatalf("sanity predicate missing from template:\n%s", q)
	}
	predicate := q[idx+len(marker):]
	for _, banned := range []string{"involvedObject", "hasToken", "composition_id_token"} {
		if strings.Contains(predicate, banned) {
			t.Errorf("composition predicate must not reference %q — it would defeat idx_log_attr_value "+
				"pruning and reintroduce a full scan:\n%s", banned, predicate)
		}
	}
	if params["composition_id"] != cid {
		t.Errorf("composition_id param = %q, want %q", params["composition_id"], cid)
	}
	if strings.Contains(q, cid) {
		t.Errorf("composition_id must NOT be interpolated into the SQL:\n%s", q)
	}
}

// TestBuildEventsQuery_NoStaleTokenParam — #21's prefilter bound a
// composition_id_token param. The predicate no longer references it, and a
// parameter bound but unused is dead weight that invites the predicate back.
func TestBuildEventsQuery_NoStaleTokenParam(t *testing.T) {
	for _, q := range []url.Values{
		url.Values{},
		mustValues(t, "composition_id=1b4e28ba-2fa1-11d2-883f-0016d3cca427"),
	} {
		sql, params, err := buildEventsQuery(q, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := params["composition_id_token"]; ok {
			t.Errorf("composition_id_token must no longer be bound, got %v", params)
		}
		if strings.Contains(sql, "composition_id_token") {
			t.Errorf("SQL must not reference the retired token param:\n%s", sql)
		}
	}
}

// TestBroadcastReportsDropsWhenTheBufferIsFull is the assertion frontend asked for on
// sse-proxy#29 and #30: a drop counter that exists but never moves is indistinguishable
// from a system that never drops. So this provokes a real drop and asserts the NUMBER
// CHANGES, not merely that the field is present.
//
// It also pins the direction that matters: a full client must not stall the others.
func TestBroadcastReportsDropsWhenTheBufferIsFull(t *testing.T) {
	h := newHub()
	// Capacity 2 so the third send has nowhere to go. Nothing reads from it, which is the
	// real shape: during a backlog drain the producer loop outruns the HTTP writer.
	full := &client{ch: make(chan sseMessage, 2), topic: ""}
	roomy := &client{ch: make(chan sseMessage, 16), topic: ""}
	h.register(full)
	h.register(roomy)

	var sent, dropped int64
	for i := 0; i < 5; i++ {
		s, d := h.broadcast(sseMessage{topic: "krateo", data: []byte("e")})
		sent += s
		dropped += d
	}

	// 5 messages x 2 clients = 10 attempts. The roomy client takes all 5; the full one
	// takes 2 and drops 3.
	if sent != 7 {
		t.Errorf("sent = %d, want 7 (5 roomy + 2 full)", sent)
	}
	if dropped != 3 {
		t.Errorf("dropped = %d, want 3 — a drop counter that never moves proves nothing", dropped)
	}
	if got := len(roomy.ch); got != 5 {
		t.Errorf("the roomy client got %d of 5: a full peer must not stall the fan-out", got)
	}
}

// TestBroadcastReportsZeroDropsWhenEveryoneKeepsUp is the negative arm. Without it the
// test above passes against an implementation that reports a constant 3.
func TestBroadcastReportsZeroDropsWhenEveryoneKeepsUp(t *testing.T) {
	h := newHub()
	c := &client{ch: make(chan sseMessage, 8), topic: ""}
	h.register(c)

	var sent, dropped int64
	for i := 0; i < 4; i++ {
		s, d := h.broadcast(sseMessage{topic: "krateo", data: []byte("e")})
		sent += s
		dropped += d
	}
	if sent != 4 || dropped != 0 {
		t.Errorf("sent/dropped = %d/%d, want 4/0", sent, dropped)
	}
}
