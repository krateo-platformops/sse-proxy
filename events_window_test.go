package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// testSince is an arbitrary positive window start for tests that only inspect
// the SQL shape.
const testSince int64 = 1791960000

// ---------------------------------------------------------------------------
// sse-proxy#25 — sargable poll cursor.
// ---------------------------------------------------------------------------

// TestPollSQL_CursorIsSargable pins that the poll cursor compares the BARE
// Timestamp column. Mutation: restore `toUnixTimestamp(Timestamp) > %d` and
// this fails (and the poll scans every k8s-events row again).
func TestPollSQL_CursorIsSargable(t *testing.T) {
	where := pollSQL[strings.Index(pollSQL, "WHERE"):]
	if strings.Contains(where, "toUnixTimestamp(Timestamp)") {
		t.Fatalf("poll WHERE wraps Timestamp in a function (defeats every index):\n%s", where)
	}
	if !strings.Contains(where, "Timestamp >= toDateTime64(%d, 9)") {
		t.Fatalf("poll WHERE lost the sargable cursor predicate:\n%s", where)
	}
}

// TestPoll_PassesCursorPlusOne pins the equivalence toUnixTimestamp(ts) > N
// <=> ts >= N+1 at the call site. Mutation: pass p.lastSeenUnix (no +1) and
// this fails — the poller would re-broadcast the last second's events.
func TestPoll_PassesCursorPlusOne(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = string(b)
	}))
	defer srv.Close()

	p := newPoller(config{clickhouseURL: srv.URL}, newHub(), nil, srv.Client())
	p.lastSeenUnix = 1791963426
	p.poll(context.Background())

	if !strings.Contains(got, "Timestamp >= toDateTime64(1791963427, 9)") {
		t.Fatalf("poll did not send cursor+1; query was:\n%s", got)
	}
}

// ---------------------------------------------------------------------------
// /events query builder: always bounded, prunable, hard LIMIT.
// ---------------------------------------------------------------------------

// TestBuildEventsQuery_AlwaysTimeBounded pins that no /events query can be
// built without a window. Mutation: let since <= 0 drop the window predicate
// (the old unbounded fallback) and this fails.
func TestBuildEventsQuery_AlwaysTimeBounded(t *testing.T) {
	for _, since := range []int64{0, -1} {
		if _, _, err := buildEventsQuery(url.Values{}, nil, since); err == nil {
			t.Fatalf("since=%d must be refused: every /events query is time-bounded", since)
		}
	}
	scoped := &nsScope{namespaces: []string{"team-a"}}
	cid := "composition_id=1b4e28ba-2fa1-11d2-883f-0016d3cca427"
	for name, tc := range map[string]struct {
		q     url.Values
		scope *nsScope
	}{
		"unscoped":             {url.Values{}, nil},
		"scoped":               {url.Values{}, scoped},
		"composition":          {mustValues(t, cid), nil},
		"scoped + composition": {mustValues(t, cid), scoped},
	} {
		t.Run(name, func(t *testing.T) {
			q, params, err := buildEventsQuery(tc.q, tc.scope, testSince)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(q, "AND TimestampTime >= toDateTime({since:Int64})") {
				t.Fatalf("window predicate missing:\n%s", q)
			}
			if params["since"] != strconv.FormatInt(testSince, 10) {
				t.Fatalf("since not bound as a parameter: %v", params)
			}
			if strings.Contains(q, strconv.FormatInt(testSince, 10)) {
				t.Fatalf("since must be bound, not interpolated:\n%s", q)
			}
			if !strings.Contains(q, "LIMIT {limit:UInt32}") {
				t.Fatalf("query must carry the bound hard LIMIT:\n%s", q)
			}
			if strings.Contains(q, "/*") {
				t.Fatalf("placeholder left in SQL:\n%s", q)
			}
		})
	}
}

// TestBuildEventsQuery_WindowUsesPrimaryKeyColumn pins the window on
// TimestampTime, the PRIMARY KEY / PARTITION column, never on a function of a
// column. Mutation: compare toUnixTimestamp(Timestamp) and this fails.
func TestBuildEventsQuery_WindowUsesPrimaryKeyColumn(t *testing.T) {
	q, _, err := buildEventsQuery(url.Values{}, nil, testSince)
	if err != nil {
		t.Fatal(err)
	}
	where := q[strings.Index(q, "WHERE"):]
	for _, banned := range []string{"toUnixTimestamp(Timestamp)", "toUnixTimestamp(TimestampTime)", "toDateTime(Timestamp) >="} {
		if strings.Contains(where, banned) {
			t.Fatalf("WHERE wraps the time column in a function (defeats pruning): %q\n%s", banned, where)
		}
	}
}

// TestBuildEventsQuery_ScopedNamespacePredicateIsPrunableAndExact pins both
// halves of namespaceFilter. Mutation: drop the ResourceAttributes predicate
// and a scoped read JSON-parses every k8s-events row in its window again; drop
// the JSON predicate and a scoped caller receives events about cluster-scoped
// objects filed in a namespace they can read.
func TestBuildEventsQuery_ScopedNamespacePredicateIsPrunableAndExact(t *testing.T) {
	q, params, err := buildEventsQuery(url.Values{}, &nsScope{namespaces: []string{"team-a"}}, testSince)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"AND ResourceAttributes['k8s.namespace.name'] IN {namespaces:Array(String)}",
		"AND JSONExtractString(Body, 'object', 'involvedObject', 'namespace') IN {namespaces:Array(String)}",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("scoped query lacks %q:\n%s", want, q)
		}
	}
	if params["namespaces"] != "['team-a']" {
		t.Errorf("namespaces param = %q", params["namespaces"])
	}
}

// ---------------------------------------------------------------------------
// window parameter + config defaults
// ---------------------------------------------------------------------------

func TestParseWindow(t *testing.T) {
	def, max := 24*time.Hour, 7*24*time.Hour
	for _, tc := range []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{"", def, false},
		{"6h", 6 * time.Hour, false},
		{"90m", 90 * time.Minute, false},
		{"168h", max, false},
		{"1000h", max, false}, // clamped, never widened past the maximum
		{"0", 0, true},
		{"-1h", 0, true},
		{"abc", 0, true},
		{"6", 0, true}, // a bare number has no unit
		{"1h; DROP TABLE otel_logs", 0, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := parseWindow(tc.raw, def, max)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseWindow(%q) err = %v, wantErr %v", tc.raw, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("parseWindow(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestConfig_EventsBoundsDefaults(t *testing.T) {
	var zero config
	if zero.eventsQueryTimeout() != defaultEventsQueryTimeout ||
		zero.eventsDefaultWindow() != defaultEventsWindow ||
		zero.eventsMaxWindow() != defaultEventsMaxWindow {
		t.Fatalf("zero config must use the defaults: %v %v %v",
			zero.eventsQueryTimeout(), zero.eventsDefaultWindow(), zero.eventsMaxWindow())
	}
	// The timeout default must leave the caller (snowplow, 10s upstream) time
	// to receive the answer.
	if defaultEventsQueryTimeout >= 10*time.Second {
		t.Fatalf("default query timeout %v is not below snowplow's 10s upstream timeout", defaultEventsQueryTimeout)
	}
	// A default wider than the maximum is clamped to it.
	c := config{eventsWindowDef: 48 * time.Hour, eventsWindowMax: 12 * time.Hour}
	if c.eventsDefaultWindow() != 12*time.Hour {
		t.Fatalf("default window must not exceed the max, got %v", c.eventsDefaultWindow())
	}
}

func TestLoadConfig_EventsBoundsFromEnv(t *testing.T) {
	t.Setenv("EVENTS_QUERY_TIMEOUT", "3s")
	t.Setenv("EVENTS_DEFAULT_WINDOW", "6h")
	t.Setenv("EVENTS_MAX_WINDOW", "48h")
	c := loadConfig()
	if c.eventsQueryTimeout() != 3*time.Second || c.eventsDefaultWindow() != 6*time.Hour || c.eventsMaxWindow() != 48*time.Hour {
		t.Fatalf("env not applied: %v %v %v", c.eventsQueryTimeout(), c.eventsDefaultWindow(), c.eventsMaxWindow())
	}
	t.Setenv("EVENTS_QUERY_TIMEOUT", "nonsense")
	if c := loadConfig(); c.eventsQueryTimeout() != defaultEventsQueryTimeout {
		t.Fatalf("unparseable timeout must fall back to the default, got %v", c.eventsQueryTimeout())
	}
}

func TestEventsWindowPlan(t *testing.T) {
	h := time.Hour
	for _, tc := range []struct {
		window time.Duration
		want   []time.Duration
	}{
		{30 * time.Minute, []time.Duration{30 * time.Minute}},
		{h, []time.Duration{h}},
		{6 * h, []time.Duration{h, 6 * h}},
		{24 * h, []time.Duration{h, 24 * h}},
		{168 * h, []time.Duration{h, 24 * h, 168 * h}},
	} {
		got := eventsWindowPlan(tc.window)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("eventsWindowPlan(%v) = %v, want %v", tc.window, got, tc.want)
		}
		if got[len(got)-1] != tc.window {
			t.Errorf("plan must end with the requested window: %v", got)
		}
	}
}

// ---------------------------------------------------------------------------
// fake ClickHouse
// ---------------------------------------------------------------------------

// fakeEventsCH answers the /events SQL the way ClickHouse would for the
// predicates this test controls: rows with ts >= since, ordered by ts DESC,
// LIMIT limit. It records every call's since and URL settings. delays[i], if
// set, delays the i-th call (or until the client goes away); errBody, if set,
// is returned with HTTP 500 instead of rows.
type fakeEventsCH struct {
	mu       sync.Mutex
	rows     []int64 // event timestamps
	delays   []time.Duration
	errBody  string
	sinces   []int64
	settings []url.Values
}

func (f *fakeEventsCH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Drain the body first: net/http only notices a client that went away (and
	// cancels r.Context()) once the request body has been consumed.
	_, _ = io.Copy(io.Discard, r.Body)
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("param_limit"))
	since, _ := strconv.ParseInt(q.Get("param_since"), 10, 64)
	f.mu.Lock()
	call := len(f.sinces)
	f.sinces = append(f.sinces, since)
	f.settings = append(f.settings, q)
	var delay time.Duration
	if call < len(f.delays) {
		delay = f.delays[call]
	}
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if f.errBody != "" {
		http.Error(w, f.errBody, http.StatusInternalServerError)
		return
	}

	var sel []int64
	for _, ts := range f.rows {
		if ts >= since {
			sel = append(sel, ts)
		}
	}
	sort.Slice(sel, func(i, j int) bool { return sel[i] > sel[j] })
	if len(sel) > limit {
		sel = sel[:limit]
	}
	enc := json.NewEncoder(w)
	for _, ts := range sel {
		_ = enc.Encode(chRow{TsUnix: ts, Reason: "R", Message: fmt.Sprint(ts)})
	}
}

func (f *fakeEventsCH) recordedSinces() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.sinces...)
}

// ---------------------------------------------------------------------------
// windowed runner: equivalence, early stop, never unbounded
// ---------------------------------------------------------------------------

// TestQueryEventsWindowed_IdenticalToRequestedWindow is the equivalence arm:
// for dense, sparse, empty and boundary-straddling histories, every limit and
// several windows, the stepped answer equals a single query over the requested
// window row-for-row. Mutation: return a step's rows when len(rows) < limit
// (i.e. drop the widening) and the sparse / boundary cases fail.
func TestQueryEventsWindowed_IdenticalToRequestedWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := int64(3600)
	n := now.Unix()

	mk := func(start, step int64, count int) []int64 {
		var out []int64
		for i := 0; i < count; i++ {
			out = append(out, start-int64(i)*step)
		}
		return out
	}
	cases := map[string][]int64{
		"empty":            nil,
		"dense-last-hour":  mk(n-1, 5, 500),                                 // >200 rows inside 1h
		"sparse-old":       {n - 3*24*h, n - 5*24*h, n - 9*24*h},            // beyond 24h, one beyond 7d
		"straddles-1h":     append(mk(n-10, 60, 50), mk(n-2*h, 60, 300)...), // 50 in 1h, rest older
		"straddles-24h":    append(mk(n-10, 600, 100), mk(n-30*h, 60, 300)...),
		"exactly-at-bound": {n - h, n - h - 1, n - 24*h, n - 24*h - 1, n - 168*h, n - 168*h - 1},
	}
	for name, rows := range cases {
		for _, window := range []time.Duration{30 * time.Minute, 24 * time.Hour, 168 * time.Hour} {
			for _, limit := range []int{1, 2, 50, 200} {
				t.Run(fmt.Sprintf("%s/window=%v/limit=%d", name, window, limit), func(t *testing.T) {
					f := &fakeEventsCH{rows: rows}
					srv := httptest.NewServer(f)
					defer srv.Close()
					cfg := config{clickhouseURL: srv.URL}
					vals := url.Values{"limit": {strconv.Itoa(limit)}}

					got, err := queryEventsWindowed(context.Background(), srv.Client(), cfg, vals, nil, now, window)
					if err != nil {
						t.Fatal(err)
					}
					if got.status != eventsStatusComplete || got.covered != window {
						t.Fatalf("status/covered = %s/%v, want complete/%v", got.status, got.covered, window)
					}
					wq, wp, _ := buildEventsQuery(vals, nil, now.Add(-window).Unix())
					want, err := queryClickHouse(context.Background(), srv.Client(), cfg, wq, wp)
					if err != nil {
						t.Fatal(err)
					}
					if want == nil {
						want = []chRow{}
					}
					if !reflect.DeepEqual(got.rows, want) {
						t.Fatalf("stepped != requested window\n got %v\nwant %v", tsOf(got.rows), tsOf(want))
					}
				})
			}
		}
	}
}

// TestQueryEventsWindowed_StopsAtFirstFullWindow: a feed with >= limit events
// in the last hour costs ONE 1h-bounded query.
func TestQueryEventsWindowed_StopsAtFirstFullWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var rows []int64
	for i := 0; i < 300; i++ {
		rows = append(rows, now.Unix()-int64(i))
	}
	f := &fakeEventsCH{rows: rows}
	srv := httptest.NewServer(f)
	defer srv.Close()

	if _, err := queryEventsWindowed(context.Background(), srv.Client(), config{clickhouseURL: srv.URL},
		url.Values{"limit": {"200"}}, nil, now, 168*time.Hour); err != nil {
		t.Fatal(err)
	}
	if s := f.recordedSinces(); len(s) != 1 || s[0] != now.Add(-time.Hour).Unix() {
		t.Fatalf("want exactly one 1h-bounded query, got sinces=%v", s)
	}
}

// TestQueryEventsWindowed_NeverUnbounded: an empty feed walks the whole plan,
// and every query it sends is bounded at or after now-window. Mutation: put
// the old unbounded step (0) back at the end of the plan and this fails.
func TestQueryEventsWindowed_NeverUnbounded(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := &fakeEventsCH{}
	srv := httptest.NewServer(f)
	defer srv.Close()

	window := 168 * time.Hour
	if _, err := queryEventsWindowed(context.Background(), srv.Client(), config{clickhouseURL: srv.URL},
		url.Values{}, nil, now, window); err != nil {
		t.Fatal(err)
	}
	sinces := f.recordedSinces()
	if len(sinces) != 3 {
		t.Fatalf("an empty feed must try 1h, 24h, then the window: sinces=%v", sinces)
	}
	for _, s := range sinces {
		if s <= 0 || s < now.Add(-window).Unix() {
			t.Fatalf("query not bounded by the requested window: since=%d (sinces=%v)", s, sinces)
		}
	}
}

// ---------------------------------------------------------------------------
// deadline → graceful answer
// ---------------------------------------------------------------------------

// TestQueryEventsWindowed_DeadlineBeforeAnyWindowIsEmptyTimeout: ClickHouse is
// too slow for even the first window. The runner answers within its deadline
// with an empty timeout result instead of an error.
func TestQueryEventsWindowed_DeadlineBeforeAnyWindowIsEmptyTimeout(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	f := &fakeEventsCH{rows: []int64{now.Unix() - 10}, delays: []time.Duration{5 * time.Second}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	res, err := queryEventsWindowed(ctx, srv.Client(), config{clickhouseURL: srv.URL}, url.Values{}, nil, now, 24*time.Hour)
	if err != nil {
		t.Fatalf("a deadline must not be an error: %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("runner did not stop at its deadline: took %v", el)
	}
	if res.status != eventsStatusTimeout || res.covered != 0 || res.rows == nil || len(res.rows) != 0 {
		t.Fatalf("want empty non-nil timeout result, got status=%s covered=%v rows=%v", res.status, res.covered, res.rows)
	}
}

// TestQueryEventsWindowed_DeadlineAfterAWindowIsPartial: the 1h window
// finishes with fewer rows than the limit, the 24h one does not finish. The
// answer is the 1h rows, labelled partial and covering 1h.
func TestQueryEventsWindowed_DeadlineAfterAWindowIsPartial(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	rows := []int64{now.Unix() - 10, now.Unix() - 20, now.Unix() - 5*3600}
	f := &fakeEventsCH{rows: rows, delays: []time.Duration{0, 5 * time.Second}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	res, err := queryEventsWindowed(ctx, srv.Client(), config{clickhouseURL: srv.URL}, url.Values{}, nil, now, 24*time.Hour)
	if err != nil {
		t.Fatalf("a deadline must not be an error: %v", err)
	}
	if res.status != eventsStatusPartial || res.covered != time.Hour {
		t.Fatalf("status/covered = %s/%v, want partial/1h", res.status, res.covered)
	}
	if got := tsOf(res.rows); !reflect.DeepEqual(got, []int64{now.Unix() - 10, now.Unix() - 20}) {
		t.Fatalf("partial rows = %v, want the two inside 1h", got)
	}
}

// TestQueryEventsWindowed_ClickHouseTimeoutCodeIsTimeout: ClickHouse enforcing
// max_execution_time answers HTTP 500 "Code: 159." — that is a deadline, not a
// failure.
func TestQueryEventsWindowed_ClickHouseTimeoutCodeIsTimeout(t *testing.T) {
	for _, body := range []string{
		"Code: 159. DB::Exception: Timeout exceeded: elapsed 4001.2 ms, maximum: 4000 ms. (TIMEOUT_EXCEEDED)",
		"Code: 160. DB::Exception: Estimated query execution time (12 seconds) is too long. (TOO_SLOW)",
	} {
		f := &fakeEventsCH{errBody: body}
		srv := httptest.NewServer(f)
		res, err := queryEventsWindowed(context.Background(), srv.Client(), config{clickhouseURL: srv.URL}, url.Values{}, nil, time.Unix(1_800_000_000, 0), 24*time.Hour)
		srv.Close()
		if err != nil || res.status != eventsStatusTimeout {
			t.Fatalf("%q: want timeout status, got status=%q err=%v", body, res.status, err)
		}
	}
}

// TestQueryEventsWindowed_OtherFailuresStayErrors: a real failure must not be
// dressed up as a timeout, and neither must a caller that went away.
func TestQueryEventsWindowed_OtherFailuresStayErrors(t *testing.T) {
	f := &fakeEventsCH{errBody: "Code: 60. DB::Exception: Unknown table expression identifier 'otel_logs'. (UNKNOWN_TABLE)"}
	srv := httptest.NewServer(f)
	defer srv.Close()
	if _, err := queryEventsWindowed(context.Background(), srv.Client(), config{clickhouseURL: srv.URL}, url.Values{}, nil, time.Now(), time.Hour); err == nil {
		t.Fatal("UNKNOWN_TABLE must be an error, not a timeout")
	}

	slow := &fakeEventsCH{delays: []time.Duration{5 * time.Second}}
	srv2 := httptest.NewServer(slow)
	defer srv2.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	if _, err := queryEventsWindowed(ctx, srv2.Client(), config{clickhouseURL: srv2.URL}, url.Values{}, nil, time.Now(), time.Hour); err == nil {
		t.Fatal("a cancelled caller must be an error, not a timeout answer")
	}
}

// TestQueryClickHouse_StopsServerSideWithTheCaller pins the settings that make
// ClickHouse stop when sse-proxy stops waiting. Mutation: drop readonly=2 and
// ClickHouse ignores cancel_http_readonly_queries_on_client_close for a POST
// (verified on krateo-057), so abandoned queries run to completion again.
func TestQueryClickHouse_StopsServerSideWithTheCaller(t *testing.T) {
	f := &fakeEventsCH{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := config{clickhouseURL: srv.URL}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := queryClickHouse(ctx, srv.Client(), cfg, "SELECT 1", map[string]string{"limit": "1", "since": "1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := queryClickHouse(context.Background(), srv.Client(), cfg, "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	withDeadline, without := f.settings[0], f.settings[1]
	f.mu.Unlock()

	for i, s := range []url.Values{withDeadline, without} {
		if s.Get("readonly") != "2" || s.Get("cancel_http_readonly_queries_on_client_close") != "1" {
			t.Fatalf("call %d: cancel-on-close settings missing: %v", i, s)
		}
	}
	met, err := strconv.ParseFloat(withDeadline.Get("max_execution_time"), 64)
	if err != nil || met <= 0 || met > 4 {
		t.Fatalf("max_execution_time = %q, want the remaining time to the 4s deadline", withDeadline.Get("max_execution_time"))
	}
	if without.Has("max_execution_time") {
		t.Fatalf("no deadline must mean no max_execution_time, got %q", without.Get("max_execution_time"))
	}
	if withDeadline.Get("param_since") != "1" {
		t.Fatalf("query parameters must still be bound: %v", withDeadline)
	}
}

func TestQueryClickHouse_ExpiredDeadlineSendsNothing(t *testing.T) {
	f := &fakeEventsCH{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := queryClickHouse(ctx, srv.Client(), config{clickhouseURL: srv.URL}, "SELECT 1", nil)
	if !isQueryTimeout(ctx, err) {
		t.Fatalf("an expired deadline must read as a timeout, got %v", err)
	}
	if n := len(f.recordedSinces()); n != 0 {
		t.Fatalf("no request may be sent past the deadline, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// handler: status headers, window param, timeout below the caller's
// ---------------------------------------------------------------------------

func serveEvents(t *testing.T, cfg config, client *http.Client, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handleEvents(cfg, client, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events?"+rawQuery, nil))
	return rec
}

func TestHandleEvents_CompleteAnswerHeaders(t *testing.T) {
	f := &fakeEventsCH{rows: []int64{time.Now().Unix() - 10}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	rec := serveEvents(t, config{clickhouseURL: srv.URL}, srv.Client(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get(eventsStatusHeader); got != eventsStatusComplete {
		t.Fatalf("%s = %q, want complete", eventsStatusHeader, got)
	}
	if got := rec.Header().Get(eventsWindowHeader); got != "604800" {
		t.Fatalf("%s = %q, want the 168h default (604800)", eventsWindowHeader, got)
	}
	expose := rec.Header().Get("Access-Control-Expose-Headers")
	if !strings.Contains(expose, eventsStatusHeader) || !strings.Contains(expose, eventsWindowHeader) {
		t.Fatalf("status headers must be exposed to cross-origin readers, got %q", expose)
	}
	var evs []SSEK8sEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &evs); err != nil || len(evs) != 1 {
		t.Fatalf("body must stay an SSEK8sEvent array: %v %s", err, rec.Body)
	}
}

// TestHandleEvents_NoWindowReadsTheWholeRetention pins Diego's 2026-11-10
// decision: a request without ?window= returns what it returned before reads
// were bounded, so no consumer has to change. A sparse history (one row 3 days
// old, one 6 days old) must come back whole, and the widest query must reach
// back the full 168h. Mutation: set defaultEventsWindow back to 24h and both
// rows vanish from the answer and the widest since moves to now-24h.
func TestHandleEvents_NoWindowReadsTheWholeRetention(t *testing.T) {
	now := time.Now().Unix()
	f := &fakeEventsCH{rows: []int64{now - 3*24*3600, now - 6*24*3600}}
	srv := httptest.NewServer(f)
	defer srv.Close()

	before := time.Now().Unix()
	rec := serveEvents(t, config{clickhouseURL: srv.URL}, srv.Client(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var evs []SSEK8sEvent
	if err := json.Unmarshal(rec.Body.Bytes(), &evs); err != nil || len(evs) != 2 {
		t.Fatalf("want both rows of a sparse 7-day history, got %d (%v): %s", len(evs), err, rec.Body)
	}
	sinces := f.recordedSinces()
	if len(sinces) != 3 {
		t.Fatalf("a sparse history must walk 1h, 24h, 168h: sinces=%v", sinces)
	}
	if d := before - 7*24*3600 - sinces[2]; d < -1 || d > 1 {
		t.Fatalf("widest default query since=%d, want ~now-168h (%d)", sinces[2], before-7*24*3600)
	}
	if defaultEventsWindow != defaultEventsMaxWindow {
		t.Fatalf("default window %v must equal the max %v (otel_logs' TTL)", defaultEventsWindow, defaultEventsMaxWindow)
	}
}

func TestHandleEvents_WindowParam(t *testing.T) {
	f := &fakeEventsCH{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := config{clickhouseURL: srv.URL}

	before := time.Now().Unix()
	rec := serveEvents(t, cfg, srv.Client(), "window=6h")
	if rec.Code != http.StatusOK || rec.Header().Get(eventsWindowHeader) != "21600" {
		t.Fatalf("window=6h: status %d, window header %q", rec.Code, rec.Header().Get(eventsWindowHeader))
	}
	sinces := f.recordedSinces()
	last := sinces[len(sinces)-1]
	if d := before - 6*3600 - last; d < -1 || d > 1 {
		t.Fatalf("widest query since=%d, want ~now-6h (%d)", last, before-6*3600)
	}

	if rec := serveEvents(t, cfg, srv.Client(), "window=1000h"); rec.Header().Get(eventsWindowHeader) != "604800" {
		t.Fatalf("window above the max must clamp to 7d, got %q", rec.Header().Get(eventsWindowHeader))
	}
	for _, bad := range []string{"window=abc", "window=0", "window=-5m"} {
		if rec := serveEvents(t, cfg, srv.Client(), bad); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", bad, rec.Code)
		}
	}
}

// TestHandleEvents_SlowClickHouseAnswersBeforeTheCallerGivesUp is the defect
// itself: a saturated ClickHouse used to hold /events until snowplow's 10s
// timeout cut it into a 5xx. Now the handler answers 200 at its own deadline,
// with an empty array and X-Events-Status: timeout.
func TestHandleEvents_SlowClickHouseAnswersBeforeTheCallerGivesUp(t *testing.T) {
	f := &fakeEventsCH{delays: []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := config{clickhouseURL: srv.URL, eventsTimeout: 200 * time.Millisecond}

	start := time.Now()
	rec := serveEvents(t, cfg, srv.Client(), "")
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("handler did not answer at its deadline: took %v", el)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 with a timeout status header", rec.Code)
	}
	if got := rec.Header().Get(eventsStatusHeader); got != eventsStatusTimeout {
		t.Fatalf("%s = %q, want timeout", eventsStatusHeader, got)
	}
	if got := rec.Header().Get(eventsWindowHeader); got != "0" {
		t.Fatalf("%s = %q, want 0", eventsWindowHeader, got)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
		t.Fatalf("body = %q, want []", body)
	}
}

func TestHandleEvents_RejectsBadCompositionBeforeClickHouse(t *testing.T) {
	f := &fakeEventsCH{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	if rec := serveEvents(t, config{clickhouseURL: srv.URL}, srv.Client(), "composition_id=nope"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if n := len(f.recordedSinces()); n != 0 {
		t.Fatalf("a bad request must not reach ClickHouse, got %d queries", n)
	}
}

// ---------------------------------------------------------------------------
// log throttle
// ---------------------------------------------------------------------------

func TestLogThrottle_OncePerIntervalWithCount(t *testing.T) {
	th := newLogThrottle(time.Minute)
	t0 := time.Unix(1_800_000_000, 0)
	if ok, n := th.allow(t0); !ok || n != 0 {
		t.Fatalf("first line must pass with 0 suppressed, got %v %d", ok, n)
	}
	for i := 1; i <= 5; i++ {
		if ok, _ := th.allow(t0.Add(time.Duration(i) * time.Second)); ok {
			t.Fatalf("line %d inside the interval must be suppressed", i)
		}
	}
	if ok, n := th.allow(t0.Add(time.Minute)); !ok || n != 5 {
		t.Fatalf("first line after the interval must pass and report 5 suppressed, got %v %d", ok, n)
	}
}

func tsOf(rows []chRow) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.TsUnix)
	}
	return out
}
