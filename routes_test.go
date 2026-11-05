package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// protectedRoutes are the real routes, so these tests pin main's wiring and not just
// the middleware: a route registered without requireBearer fails them.
var protectedRoutes = []string{"/events", "/audit", "/notifications", "/notifications/some-composition"}

func testRouter() http.Handler {
	return newRouter(config{}, testAuthConfig(), nil, http.DefaultClient, nil, nil)
}

func TestRouter_RefusesQueryToken(t *testing.T) {
	router := testRouter()
	for _, route := range protectedRoutes {
		for _, param := range []string{"access_token", "token"} {
			target := route + "?" + param + "=" + mint(t, time.Hour)
			for name, setup := range map[string]func(*http.Request){
				"alone":               func(*http.Request) {},
				"with a valid bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour)) },
				"with a valid cookie": func(r *http.Request) {
					r.AddCookie(&http.Cookie{Name: defaultSessionCookie, Value: mint(t, time.Hour)})
				},
			} {
				t.Run(route+" "+param+" "+name, func(t *testing.T) {
					r := httptest.NewRequest(http.MethodGet, target, nil)
					setup(r)
					w := httptest.NewRecorder()
					router.ServeHTTP(w, r)
					if w.Code != http.StatusUnauthorized {
						t.Fatalf("status = %d, want 401", w.Code)
					}
					if !strings.Contains(w.Body.String(), "query string") {
						t.Errorf("body should name the cause: %s", w.Body.String())
					}
				})
			}
		}
	}
}

func TestRouter_RequiresCredentials(t *testing.T) {
	router := testRouter()
	for _, route := range protectedRoutes {
		t.Run(route, func(t *testing.T) {
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route, nil))
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			// Without this a browser reports a network error instead of the 401.
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
				t.Errorf("Allow-Origin = %q on a 401", got)
			}
		})
	}
}

// The cross-origin fetch the portal makes: a preflight has no credentials, so it
// must succeed before authentication or the stream never opens.
func TestRouter_AnswersPreflightWithoutCredentials(t *testing.T) {
	router := testRouter()
	for _, route := range protectedRoutes {
		t.Run(route, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodOptions, route, nil)
			r.Header.Set("Origin", "http://portal.example")
			r.Header.Set("Access-Control-Request-Method", "GET")
			r.Header.Set("Access-Control-Request-Headers", "authorization,traceparent,tracestate")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", w.Code)
			}
			got := w.Header().Get("Access-Control-Allow-Headers")
			for _, want := range []string{"Authorization", "traceparent", "tracestate"} {
				if !strings.Contains(got, want) {
					t.Errorf("Allow-Headers = %q, missing %s", got, want)
				}
			}
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
				t.Errorf("Allow-Origin = %q", got)
			}
		})
	}
}

// Only a real preflight skips authentication.
func TestRouter_PlainOptionsStillRequiresCredentials(t *testing.T) {
	router := testRouter()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodOptions, "/notifications", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestRouter_HealthIsPublic(t *testing.T) {
	w := httptest.NewRecorder()
	testRouter().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

// A token must not slip past the refusal by being awkwardly formed: url.Query drops pairs
// it can't parse and is case-sensitive.
func TestRouter_RefusesMalformedQueryToken(t *testing.T) {
	router := testRouter()
	tok := mint(t, time.Hour)
	for _, query := range []string{
		"access_token=" + tok + ";x=1",
		"x=1;token=" + tok,
		"access_token=JW%zzT",
		"ACCESS_TOKEN=" + tok,
		"Token=" + tok,
		"access%5Ftoken=" + tok,
		"token",
		"limit=5&token=" + tok,
	} {
		t.Run(query[:min(len(query), 20)], func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/events", nil)
			r.URL.RawQuery = query
			r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
		})
	}
}

func TestQueryTokenParam_LeavesOtherParametersAlone(t *testing.T) {
	for _, query := range []string{"", "limit=5", "composition_id=abc&limit=200", "tokens=1", "my_token=1", "x=token"} {
		r := httptest.NewRequest(http.MethodGet, "/events", nil)
		r.URL.RawQuery = query
		if got := queryTokenParam(r); got != "" {
			t.Errorf("%q: got %q", query, got)
		}
	}
}

type emptyJSONTransport struct{}

func (emptyJSONTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
}

// The other router tests only prove refusals: a route wired to a handler that always
// returns 401 would pass them. A valid token must get through on every protected route.
func TestRouter_ValidTokenReachesTheHandler(t *testing.T) {
	router := newRouter(config{}, testAuthConfig(), newHub(), &http.Client{Transport: emptyJSONTransport{}}, nil, nil)
	for _, route := range protectedRoutes {
		t.Run(route, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			r := httptest.NewRequest(http.MethodGet, route, nil).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			// Only 401 means it was stopped. /audit answers 503 here, from inside its handler,
			// because this router has no scoper and it fails closed.
			if w.Code == http.StatusUnauthorized {
				t.Fatalf("status = 401: a valid token did not reach the handler (body=%s)", w.Body.String())
			}
		})
	}
}
