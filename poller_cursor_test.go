package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// sse-proxy#30: the stream is live events, so a boot must not start from the past.
func TestPoller_StartsLive(t *testing.T) {
	before := time.Now()
	p := newPoller(config{}, newHub(), nil, nil)
	if got, want := p.lastSeenUnix, before.Add(-liveLookback-time.Second).Unix(); got < want {
		t.Fatalf("cursor starts at %d, older than the %s lookback (%d)", got, liveLookback, want)
	}
}

func TestTick_IdleKeepsTheCursorLiveWithoutQuerying(t *testing.T) {
	var queries atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { queries.Add(1) }))
	defer srv.Close()
	p := newPoller(config{clickhouseURL: srv.URL}, newHub(), nil, srv.Client())
	p.lastSeenUnix = 1000

	now := time.Unix(1_000_000, 0)
	p.tick(context.Background(), now)

	if want := now.Add(-liveLookback).Unix(); p.lastSeenUnix != want {
		t.Fatalf("idle cursor = %d, want %d", p.lastSeenUnix, want)
	}
	if queries.Load() != 0 {
		t.Fatalf("idle tick ran %d queries", queries.Load())
	}
}

// A cursor already inside the lookback must not move back, or a reconnect replays rows.
func TestTick_IdleNeverMovesTheCursorBack(t *testing.T) {
	p := newPoller(config{}, newHub(), nil, nil)
	now := time.Unix(1_000_000, 0)
	p.lastSeenUnix = now.Unix() - 1
	p.tick(context.Background(), now)
	if p.lastSeenUnix != now.Unix()-1 {
		t.Fatalf("cursor moved back to %d", p.lastSeenUnix)
	}
}

func TestTick_PollsFromTheCursorWhenAClientIsConnected(t *testing.T) {
	var query atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		query.Store(string(b) + r.URL.RawQuery)
	}))
	defer srv.Close()
	h := newHub()
	h.register(&client{ch: make(chan sseMessage, 1)})
	p := newPoller(config{clickhouseURL: srv.URL}, h, nil, srv.Client())
	p.lastSeenUnix = 1791963426

	p.tick(context.Background(), time.Now())

	q, _ := query.Load().(string)
	if !strings.Contains(q, fmt.Sprint(p.lastSeenUnix+1)) {
		t.Fatalf("query doesn't start from the cursor: %q", q)
	}
}

// One row with a composition id is published on two topics: a global client gets it once.
func TestBroadcast_GlobalClientGetsARowOnce(t *testing.T) {
	h := newHub()
	c := &client{ch: make(chan sseMessage, 4)}
	h.register(c)
	h.broadcast(sseMessage{topic: globalTopic, data: []byte("e")})
	h.broadcast(sseMessage{topic: "comp-123", data: []byte("e")})
	if got := len(c.ch); got != 1 {
		t.Fatalf("global client got the row %d times", got)
	}
}

// sse-proxy#30: a client that has kept up takes a whole poll without a drop.
func TestNewClient_HoldsAFullPoll(t *testing.T) {
	h := newHub()
	c := newClient("", nil)
	h.register(c)
	var dropped int64
	for i := 0; i < pollLimit; i++ {
		_, d := h.broadcast(sseMessage{topic: globalTopic, data: []byte("e")})
		dropped += d
	}
	if dropped != 0 {
		t.Fatalf("a full poll of %d rows dropped %d", pollLimit, dropped)
	}
}

func TestPollSQL_UsesPollLimit(t *testing.T) {
	q := fmt.Sprintf(pollSQL, int64(1), pollLimit)
	if !strings.Contains(q, fmt.Sprintf("LIMIT %d", pollLimit)) || strings.Contains(q, "%!") {
		t.Fatalf("poll query doesn't carry pollLimit: %q", q[len(q)-60:])
	}
}
