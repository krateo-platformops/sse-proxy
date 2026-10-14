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

// ---------------------------------------------------------------------------
// sse-proxy#25 — sargable poll cursor + newest-window-first /events.
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

func TestBuildEventsQueryWindow_BoundIsSargableAndBound(t *testing.T) {
	q, params, err := buildEventsQueryWindow(url.Values{}, nil, 1791960000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "AND Timestamp >= toDateTime64({since:Int64}, 9)") {
		t.Fatalf("window predicate missing or not sargable:\n%s", q)
	}
	if params["since"] != "1791960000" {
		t.Fatalf("since not bound as a parameter: %v", params)
	}
	if strings.Contains(q, windowPredicatePlaceholder) {
		t.Fatalf("placeholder left in SQL:\n%s", q)
	}

	u, params, err := buildEventsQueryWindow(url.Values{}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(u, "{since") || params["since"] != "" || strings.Contains(u, windowPredicatePlaceholder) {
		t.Fatalf("unbounded query must carry no window:\n%s\n%v", u, params)
	}
	// buildEventsQuery stays the unbounded form, byte-identical to the window-0 form.
	b, _, _ := buildEventsQuery(url.Values{}, nil)
	if b != u {
		t.Fatalf("buildEventsQuery != buildEventsQueryWindow(...,0)")
	}
}

func TestEventsWindows_EndUnbounded(t *testing.T) {
	if len(eventsWindows) == 0 || eventsWindows[len(eventsWindows)-1] != 0 {
		t.Fatalf("eventsWindows must end with the unbounded window (0): %v", eventsWindows)
	}
	for i := 1; i < len(eventsWindows)-1; i++ {
		if eventsWindows[i] <= eventsWindows[i-1] {
			t.Fatalf("bounded windows must widen: %v", eventsWindows)
		}
	}
}

// fakeEventsCH answers the /events SQL the way ClickHouse would for the
// predicates this test controls: rows with ts >= since (when bound), ordered
// by ts DESC, LIMIT limit. It records every call's since.
type fakeEventsCH struct {
	mu     sync.Mutex
	rows   []int64 // event timestamps
	sinces []int64
}

func (f *fakeEventsCH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("param_limit"))
	var since int64
	if s := q.Get("param_since"); s != "" {
		since, _ = strconv.ParseInt(s, 10, 64)
	}
	f.mu.Lock()
	f.sinces = append(f.sinces, since)
	f.mu.Unlock()

	var sel []int64
	for _, ts := range f.rows {
		if since == 0 || ts >= since {
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

// TestQueryEventsWindowed_IdenticalToUnbounded is the equivalence arm: for
// dense, sparse, empty and boundary-straddling histories, every limit, the
// windowed answer equals the unbounded answer row-for-row. Mutation: return a
// window's rows when len(rows) < limit (i.e. drop the widening) and the
// sparse / boundary cases fail.
func TestQueryEventsWindowed_IdenticalToUnbounded(t *testing.T) {
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
		"sparse-old":       {n - 3*24*h, n - 5*24*h},                        // only beyond 24h
		"straddles-1h":     append(mk(n-10, 60, 50), mk(n-2*h, 60, 300)...), // 50 in 1h, rest older
		"straddles-24h":    append(mk(n-10, 600, 100), mk(n-30*h, 60, 300)...),
		"exactly-at-bound": {n - h, n - h - 1, n - 24*h, n - 24*h - 1},
	}
	for name, rows := range cases {
		for _, limit := range []int{1, 2, 20, 50, 200} {
			t.Run(fmt.Sprintf("%s/limit=%d", name, limit), func(t *testing.T) {
				f := &fakeEventsCH{rows: rows}
				srv := httptest.NewServer(f)
				defer srv.Close()
				cfg := config{clickhouseURL: srv.URL}
				vals := url.Values{"limit": {strconv.Itoa(limit)}}

				got, err := queryEventsWindowed(context.Background(), srv.Client(), cfg, vals, nil, now)
				if err != nil {
					t.Fatal(err)
				}
				uq, up, _ := buildEventsQuery(vals, nil)
				want, err := queryClickHouse(context.Background(), srv.Client(), cfg, uq, up)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("windowed != unbounded\n got %v\nwant %v", tsOf(got), tsOf(want))
				}
			})
		}
	}
}

// TestQueryEventsWindowed_StopsAtFirstFullWindow pins the point of the change:
// a composition with >= limit events in the last hour costs ONE bounded query,
// never the unbounded scan.
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
		url.Values{"limit": {"200"}}, nil, now); err != nil {
		t.Fatal(err)
	}
	if len(f.sinces) != 1 || f.sinces[0] != now.Add(-time.Hour).Unix() {
		t.Fatalf("want exactly one 1h-bounded query, got sinces=%v", f.sinces)
	}
}

func tsOf(rows []chRow) []int64 {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.TsUnix)
	}
	return out
}
