package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The composition object on each event: {id, namespace, name}, present only
// when the event carries a composition id, with namespace and name omitted when
// the collector did not stamp them (rows older than otel-collector 1.0.8).

const testCompositionID = "0a1b2c3d-0000-4000-8000-00000000c0de"

// compositionJSON returns the raw "composition" member of an encoded event,
// and whether it is present at all.
func compositionJSON(t *testing.T, evt SSEK8sEvent) (string, bool) {
	t.Helper()
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["compositionId"]; !ok {
		t.Fatalf("compositionId must stay on every event for compatibility: %s", b)
	}
	raw, ok := m["composition"]
	return string(raw), ok
}

// TestToSSEK8sEvent_Composition covers the three row shapes. Mutations: drop
// `omitempty` from EventComposition's namespace/name and the id-only case
// fails (it would send "name":""); drop the CompositionID guard in
// toSSEK8sEvent and the no-composition case fails (it would send {"id":""}).
func TestToSSEK8sEvent_Composition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		row         chRow
		want        string // exact JSON of the composition member
		wantPresent bool
	}{
		{
			name:        "stamped by otel-collector 1.0.8",
			row:         chRow{CompositionID: testCompositionID, CompositionName: "devday-milan", CompositionNamespace: "demo-system"},
			want:        `{"id":"` + testCompositionID + `","namespace":"demo-system","name":"devday-milan"}`,
			wantPresent: true,
		},
		{
			name:        "older row: id only",
			row:         chRow{CompositionID: testCompositionID},
			want:        `{"id":"` + testCompositionID + `"}`,
			wantPresent: true,
		},
		{
			name:        "name without namespace",
			row:         chRow{CompositionID: testCompositionID, CompositionName: "devday-milan"},
			want:        `{"id":"` + testCompositionID + `","name":"devday-milan"}`,
			wantPresent: true,
		},
		{
			name:        "no composition",
			row:         chRow{},
			wantPresent: false,
		},
		{
			// A name with no id identifies nothing a client can act on.
			name:        "name but no id",
			row:         chRow{CompositionName: "orphan", CompositionNamespace: "demo-system"},
			wantPresent: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := tc.row.toSSEK8sEvent()
			if evt.CompositionID != tc.row.CompositionID {
				t.Fatalf("compositionId = %q, want %q", evt.CompositionID, tc.row.CompositionID)
			}
			got, present := compositionJSON(t, evt)
			if present != tc.wantPresent {
				t.Fatalf("composition present = %v, want %v (got %s)", present, tc.wantPresent, got)
			}
			if present && got != tc.want {
				t.Fatalf("composition = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestCompositionColumns_InBothQueries pins that /events and the poller (the
// /notifications stream) both select the two new LogAttributes keys under the
// aliases chRow decodes. Mutation: delete either line from either query and
// this fails.
func TestCompositionColumns_InBothQueries(t *testing.T) {
	for name, sql := range map[string]string{"pollSQL": pollSQL, "eventsSQLTemplate": eventsSQLTemplate} {
		sel := sql[:strings.Index(sql, "FROM otel_logs")]
		for _, want := range []string{
			"ifNull(LogAttributes['krateo.io/composition-id'], '')",
			"ifNull(LogAttributes['krateo.io/composition-name'], '')",
			"ifNull(LogAttributes['krateo.io/composition-namespace'], '')",
			"AS composition_name,",
			"AS composition_namespace,",
		} {
			if !strings.Contains(sel, want) {
				t.Errorf("%s SELECT lacks %q", name, want)
			}
		}
	}
}

// compositionRowsCH answers every query with the same two JSONEachRow lines,
// spelled with the SQL aliases (not chRow), so a tag that drifts from its
// alias is caught.
func compositionRowsCH(t *testing.T) *httptest.Server {
	t.Helper()
	now := time.Now().Unix()
	lines := strings.Join([]string{
		`{"ts_unix":` + itoa64(now-10) + `,"composition_id":"` + testCompositionID + `","composition_name":"devday-milan","composition_namespace":"demo-system","obj_namespace":"demo-system","reason":"Synced","type":"Normal"}`,
		`{"ts_unix":` + itoa64(now-20) + `,"composition_id":"` + testCompositionID + `","composition_name":"","composition_namespace":"","obj_namespace":"demo-system","reason":"Old","type":"Normal"}`,
		`{"ts_unix":` + itoa64(now-30) + `,"composition_id":"","composition_name":"","composition_namespace":"","obj_namespace":"kube-system","reason":"Pulled","type":"Normal"}`,
	}, "\n") + "\n"
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, lines)
	}))
}

func itoa64(n int64) string { b, _ := json.Marshal(n); return string(b) }

// wantCompositions is what compositionRowsCH's three rows must become.
var wantCompositions = []string{
	`{"id":"` + testCompositionID + `","namespace":"demo-system","name":"devday-milan"}`,
	`{"id":"` + testCompositionID + `"}`,
	"", // absent
}

func checkCompositions(t *testing.T, where string, evs []json.RawMessage) {
	t.Helper()
	if len(evs) != len(wantCompositions) {
		t.Fatalf("%s: %d events, want %d", where, len(evs), len(wantCompositions))
	}
	for i, raw := range evs {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("%s: event %d: %v", where, i, err)
		}
		got := string(m["composition"])
		if got != wantCompositions[i] {
			t.Errorf("%s: event %d composition = %q, want %q (%s)", where, i, got, wantCompositions[i], raw)
		}
		if _, ok := m["compositionId"]; !ok {
			t.Errorf("%s: event %d lost compositionId", where, i)
		}
	}
}

func TestHandleEvents_ReturnsCompositionPerEvent(t *testing.T) {
	srv := compositionRowsCH(t)
	defer srv.Close()
	rec := serveEvents(t, config{clickhouseURL: srv.URL}, srv.Client(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var evs []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &evs); err != nil {
		t.Fatal(err)
	}
	checkCompositions(t, "/events", evs)
}

// TestPoll_StreamCarriesComposition: the /notifications stream builds its
// events from the same columns, through the poller.
func TestPoll_StreamCarriesComposition(t *testing.T) {
	srv := compositionRowsCH(t)
	defer srv.Close()
	h := newHub()
	c := newClient("", nil)
	h.register(c)
	p := newPoller(config{clickhouseURL: srv.URL}, h, nil, srv.Client())
	p.poll(context.Background())

	var evs []json.RawMessage
	for len(c.ch) > 0 {
		evs = append(evs, (<-c.ch).data)
	}
	checkCompositions(t, "/notifications", evs)
}
