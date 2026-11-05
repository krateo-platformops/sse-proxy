package main

import (
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
			r.Header.Set("Access-Control-Request-Headers", "authorization")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204", w.Code)
			}
			if got := w.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(got, "Authorization") {
				t.Errorf("Allow-Headers = %q", got)
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
