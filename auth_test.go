package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

const testSigningKey = "test-shared-secret"

// mint creates a token exactly as authn/snowplow do (jwtutil.CreateToken,
// HS256, same KrateoClaims). A negative duration yields an already-expired one.
func mint(t *testing.T, dur time.Duration) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   "alice",
		Groups:     []string{"devs"},
		Duration:   dur,
		SigningKey: testSigningKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func TestRequireBearer_Enabled(t *testing.T) {
	a := authConfig{signingKey: testSigningKey, sessionCookie: defaultSessionCookie}
	h := a.requireBearer(okHandler)

	cases := []struct {
		name       string
		setup      func(r *http.Request)
		wantStatus int
	}{
		{
			name:       "valid token via header",
			setup:      func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour)) },
			wantStatus: http.StatusOK,
		},
		{
			name: "valid token via cookie",
			setup: func(r *http.Request) {
				r.AddCookie(&http.Cookie{Name: defaultSessionCookie, Value: mint(t, time.Hour)})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing header",
			setup:      func(r *http.Request) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "expired token",
			setup:      func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+mint(t, -time.Hour)) },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "garbage token",
			setup:      func(r *http.Request) { r.Header.Set("Authorization", "Bearer not.a.jwt") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "token signed with wrong key",
			setup: func(r *http.Request) {
				other, _ := jwtutil.CreateToken(jwtutil.CreateTokenOptions{Username: "x", Duration: time.Hour, SigningKey: "different-secret"})
				r.Header.Set("Authorization", "Bearer "+other)
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "query param token is NOT accepted on /events",
			setup:      func(r *http.Request) {},
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := "/events"
			if tc.name == "query param token is NOT accepted on /events" {
				target = "/events?access_token=" + mint(t, time.Hour)
			}
			r := httptest.NewRequest(http.MethodGet, target, nil)
			tc.setup(r)
			w := httptest.NewRecorder()
			h(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}

func TestRequireSSEToken_Enabled(t *testing.T) {
	a := authConfig{signingKey: testSigningKey, sessionCookie: defaultSessionCookie}
	h := a.requireSSEToken(okHandler)

	cases := []struct {
		name       string
		target     string
		setup      func(r *http.Request)
		wantStatus int
	}{
		{
			name:       "valid via query access_token",
			target:     "/notifications?access_token=" + mint(t, time.Hour),
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid via query token",
			target:     "/notifications?token=" + mint(t, time.Hour),
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid via header",
			target:     "/notifications",
			setup:      func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+mint(t, time.Hour)) },
			wantStatus: http.StatusOK,
		},
		{
			name:   "valid via cookie",
			target: "/notifications",
			setup: func(r *http.Request) {
				r.AddCookie(&http.Cookie{Name: defaultSessionCookie, Value: mint(t, time.Hour)})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "expired via query",
			target:     "/notifications?access_token=" + mint(t, -time.Hour),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "no credentials",
			target:     "/notifications",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.target, nil)
			if tc.setup != nil {
				tc.setup(r)
			}
			w := httptest.NewRecorder()
			h(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}
