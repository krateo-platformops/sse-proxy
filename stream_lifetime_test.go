package main

// A /notifications stream is authorized once, at connect, but can stay open for hours. These tests
// pin the two things that bound it to what its caller may still see: the stream ends when the token
// it was opened with expires, and with RBAC scoping on its scope is re-resolved while it is open.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

// streamRecorder is a ResponseWriter and Flusher whose body can be read while the handler is still
// writing to it.
type streamRecorder struct {
	mu     sync.Mutex
	header http.Header
	body   bytes.Buffer
	code   int
}

func newStreamRecorder() *streamRecorder { return &streamRecorder{header: http.Header{}} }

func (s *streamRecorder) Header() http.Header { return s.header }
func (s *streamRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.code == 0 {
		s.code = code
	}
}
func (s *streamRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.body.Write(p)
}
func (s *streamRecorder) Flush() {}
func (s *streamRecorder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.String()
}

// serveStream runs h on r in the background and returns the recorder and a channel closed when the
// handler returns.
func serveStream(h http.Handler, r *http.Request) (*streamRecorder, <-chan struct{}) {
	w := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(w, r)
	}()
	return w, done
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNotifications_StreamEndsAtTokenExpiry(t *testing.T) {
	router := newRouter(config{}, testAuthConfig(), newHub(), &http.Client{Transport: emptyJSONTransport{}}, nil, nil)
	// exp has one-second resolution, so a 1.5 s token expires between 0.5 s and 1.5 s from now.
	r := httptest.NewRequest(http.MethodGet, "/notifications", nil)
	r.Header.Set("Authorization", "Bearer "+mint(t, 1500*time.Millisecond))

	w, done := serveStream(router, r)
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("the stream was still open 4 s after its token expired")
	}
	body := w.String()
	if !strings.HasPrefix(body, ": connected\n\n") {
		t.Fatalf("the stream did not open: body=%q", body)
	}
	if !strings.HasSuffix(body, ": token expired\n\n") {
		t.Fatalf("the stream did not end with the token-expired comment: body=%q", body)
	}
}

func TestNotifications_StreamWithLiveTokenStaysOpen(t *testing.T) {
	router := newRouter(config{}, testAuthConfig(), newHub(), &http.Client{Transport: emptyJSONTransport{}}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/notifications", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour))

	w, done := serveStream(router, r)
	select {
	case <-done:
		t.Fatalf("the stream ended on its own with a token valid for an hour: body=%q", w.String())
	case <-time.After(1500 * time.Millisecond):
	}
	cancel()
	<-done
	if strings.Contains(w.String(), "token expired") {
		t.Fatalf("a live token's stream reported expiry: body=%q", w.String())
	}
}

// mutableKubeAPI answers like fakeKubeAPI for alice, except that whether she may list events in
// team-b, and whether the API answers at all, can change while a stream is open.
func mutableKubeAPI(t *testing.T, allowTeamB, failing *atomic.Bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
			{"metadata": map[string]any{"name": "team-a"}},
			{"metadata": map[string]any{"name": "team-b"}},
		}})
	})
	mux.HandleFunc("/apis/authorization.k8s.io/v1/subjectaccessreviews", func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		var sar struct {
			Spec struct {
				ResourceAttributes struct {
					Namespace string `json:"namespace"`
				} `json:"resourceAttributes"`
			} `json:"spec"`
		}
		_ = json.NewDecoder(r.Body).Decode(&sar)
		ns := sar.Spec.ResourceAttributes.Namespace
		allowed := ns == "team-a" || (ns == "team-b" && allowTeamB.Load())
		_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"allowed": allowed}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// aliceStream opens /notifications as alice straight on handleSSE (identity already verified, no
// token expiry), with a scoper whose cache TTL is ttl.
func aliceStream(t *testing.T, h *hub, scoper *rbacScoper) (*streamRecorder, <-chan struct{}, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/notifications", nil)
	r = r.WithContext(withUserInfo(ctx, jwtutil.UserInfo{Username: "alice"}))
	w, done := serveStream(handleSSE(h, scoper), r)
	waitFor(t, "the stream to register", 2*time.Second, func() bool { return h.count() == 1 })
	t.Cleanup(func() { cancel(); <-done })
	return w, done, cancel
}

func TestNotifications_RevokedNamespaceStopsArrivingWhileOpen(t *testing.T) {
	var allowTeamB, failing atomic.Bool
	allowTeamB.Store(true)
	const ttl = 100 * time.Millisecond
	scoper := newTestScoper(mutableKubeAPI(t, &allowTeamB, &failing), ttl)
	h := newHub()
	w, _, _ := aliceStream(t, h, scoper)

	h.broadcast(sseMessage{topic: globalTopic, data: []byte(`"b-before"`), namespace: "team-b"})
	waitFor(t, "team-b's event while alice may see team-b", 2*time.Second, func() bool {
		return strings.Contains(w.String(), "b-before")
	})

	allowTeamB.Store(false)
	// The scoper may serve a cache entry up to one TTL old, so the narrower scope is in place
	// within two TTLs; wait for three.
	time.Sleep(3 * ttl)
	h.broadcast(sseMessage{topic: globalTopic, data: []byte(`"b-after"`), namespace: "team-b"})
	h.broadcast(sseMessage{topic: globalTopic, data: []byte(`"a-after"`), namespace: "team-a"})
	waitFor(t, "team-a's event after the revocation", 2*time.Second, func() bool {
		return strings.Contains(w.String(), "a-after")
	})
	if strings.Contains(w.String(), "b-after") {
		t.Fatalf("an event from a namespace alice no longer may see reached her open stream: body=%q", w.String())
	}
}

func TestNotifications_StreamEndsWhenScopeCannotBeResolved(t *testing.T) {
	var allowTeamB, failing atomic.Bool
	const ttl = 100 * time.Millisecond
	scoper := newTestScoper(mutableKubeAPI(t, &allowTeamB, &failing), ttl)
	h := newHub()
	w, done, _ := aliceStream(t, h, scoper)

	failing.Store(true)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the stream stayed open after its scope could no longer be resolved")
	}
	if !strings.HasSuffix(w.String(), ": scope unavailable\n\n") {
		t.Fatalf("the stream did not end with the scope-unavailable comment: body=%q", w.String())
	}
}

func TestNotifications_QueuedMessageOutsideNarrowedScopeIsNotWritten(t *testing.T) {
	var allowTeamB, failing atomic.Bool
	allowTeamB.Store(true)
	// A long TTL, so no rescope runs during the test: the scope is narrowed by hand below.
	scoper := newTestScoper(mutableKubeAPI(t, &allowTeamB, &failing), time.Hour)
	h := newHub()
	w, _, _ := aliceStream(t, h, scoper)

	h.mu.RLock()
	var c *client
	for cl := range h.clients {
		c = cl
	}
	h.mu.RUnlock()
	// The hub queued this while team-b was allowed; the scope narrows before the stream writes it.
	c.scope.Store(&nsScope{namespaces: []string{"team-a"}})
	c.ch <- sseMessage{topic: globalTopic, data: []byte(`"queued-b"`), namespace: "team-b"}
	c.ch <- sseMessage{topic: globalTopic, data: []byte(`"queued-a"`), namespace: "team-a"}
	waitFor(t, "the queued team-a event", 2*time.Second, func() bool {
		return strings.Contains(w.String(), "queued-a")
	})
	if strings.Contains(w.String(), "queued-b") {
		t.Fatalf("a message queued before the scope narrowed was written after: body=%q", w.String())
	}
}

func TestRequireBearer_CarriesTokenExpiry(t *testing.T) {
	var got time.Time
	var ok bool
	h := testAuthConfig().requireBearer(func(w http.ResponseWriter, r *http.Request) {
		got, ok = tokenExpiryFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	r := httptest.NewRequest(http.MethodGet, "/events", nil)
	r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour))
	h(httptest.NewRecorder(), r)
	if !ok {
		t.Fatal("no token expiry on the request context")
	}
	if d := time.Until(got); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("token expiry is %s from now, want about an hour", d)
	}
}
