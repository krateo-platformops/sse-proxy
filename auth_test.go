package main

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

// authn now signs RS256, so tests mint tokens with an RSA private key and verify
// them against the matching public key via an in-memory StaticKeySource (the
// same adapter plumbing exposes for the JWKS-less form) — no mock JWKS server
// needed. testKeyID is the "kid" stamped in every minted token's header.
const testKeyID = "test-kid"

var testPrivateKey = mustGenerateRSAKey()

func mustGenerateRSAKey() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic("generating RSA test key: " + err.Error())
	}
	return k
}

// testAuthConfig builds an authConfig whose key source trusts testPrivateKey's
// public key for every kid (StaticKeySource) — the enabled/enforcing state.
func testAuthConfig() authConfig {
	return authConfig{
		keys:          jwtutil.NewStaticKeySource(&testPrivateKey.PublicKey),
		jwksURL:       "static://test",
		sessionCookie: defaultSessionCookie,
	}
}

// mint creates a token exactly as authn does (jwtutil.CreateToken, RS256, same
// KrateoClaims). A negative duration yields an already-expired one.
func mint(t *testing.T, dur time.Duration) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   "alice",
		Groups:     []string{"devs"},
		Duration:   dur,
		KeyID:      testKeyID,
		PrivateKey: testPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

func okHandler(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func TestRequireBearer_Enabled(t *testing.T) {
	a := testAuthConfig()
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
			name: "token signed with a different key",
			setup: func(r *http.Request) {
				otherKey := mustGenerateRSAKey()
				other, _ := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
					Username: "x", Duration: time.Hour, KeyID: testKeyID, PrivateKey: otherKey,
				})
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
	a := testAuthConfig()
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
