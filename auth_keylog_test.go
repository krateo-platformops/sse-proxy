package main

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
)

// A 503 from requireBearer (authn's signing key unavailable) is our fault, and
// it used to be logged nowhere. It now logs a WARN with the reason and the kid,
// at most once a minute per reason, and never the token.

const keyLogMsg = "token signing key unavailable; answered 503"

// jwksServer publishes testPrivateKey's public key under kid.
func jwksServer(t *testing.T, kid string) *httptest.Server {
	t.Helper()
	pub := testPrivateKey.PublicKey
	doc := map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": kid,
		"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// jwksAuthConfig verifies against a real JWKS key source at url, with its own
// rate-limited log.
func jwksAuthConfig(url string, kl *keyUnavailableLog) authConfig {
	return authConfig{
		keys:          jwtutil.NewJWKSKeySource(url, jwtutil.WithJWKSMinRefreshInterval(0), jwtutil.WithJWKSRequestTimeout(time.Second)),
		jwksURL:       url,
		sessionCookie: defaultSessionCookie,
		keyLog:        kl,
	}
}

func serveWithToken(a authConfig, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/events", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	a.requireBearer(okHandler)(w, r)
	return w
}

func keyLogLines(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range logs.records(t) {
		if r["msg"] == keyLogMsg {
			out = append(out, r)
		}
	}
	return out
}

// assertNoToken fails if the token, or any of its three segments, reached the
// log. The header segment is checked too: it is token material, even though
// the kid it carries is logged on its own.
func assertNoToken(t *testing.T, logs *syncBuffer, token string) {
	t.Helper()
	out := logs.String()
	if strings.Contains(out, token) {
		t.Fatal("the token was logged")
	}
	for i, seg := range strings.Split(token, ".") {
		if len(seg) > 8 && strings.Contains(out, seg) {
			t.Fatalf("token segment %d was logged", i)
		}
	}
}

// TestRequireBearer_UnknownKidLogsWarn: authn's JWKS is reachable but holds no
// key for the token's kid. Mutation: remove the logKeyUnavailable call from
// writeAuthError and no line is written.
func TestRequireBearer_UnknownKidLogsWarn(t *testing.T) {
	logs := captureLogs(t)
	a := jwksAuthConfig(jwksServer(t, "rotated-kid").URL, newKeyUnavailableLog())
	tok := mint(t, time.Hour) // kid = testKeyID

	if w := serveWithToken(a, tok); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	lines := keyLogLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("want 1 WARN line, got %d:\n%s", len(lines), logs)
	}
	l := lines[0]
	if l["level"] != "WARN" || l["reason"] != keyReasonUnknownKid || l["kid"] != testKeyID || l["route"] == nil {
		t.Fatalf("unexpected line: %v", l)
	}
	assertNoToken(t, logs, tok)
}

// TestRequireBearer_JWKSUnreachableLogsWarn: the key set cannot be fetched.
// Mutation: have keyUnavailableReason always return keyReasonUnknownKid and the
// reason check fails.
func TestRequireBearer_JWKSUnreachableLogsWarn(t *testing.T) {
	logs := captureLogs(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	a := jwksAuthConfig(url, newKeyUnavailableLog())
	tok := mint(t, time.Hour)

	if w := serveWithToken(a, tok); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	lines := keyLogLines(t, logs)
	if len(lines) != 1 || lines[0]["reason"] != keyReasonJWKSUnavailable || lines[0]["kid"] != testKeyID {
		t.Fatalf("want one jwks_unavailable line with the kid, got:\n%s", logs)
	}
	assertNoToken(t, logs, tok)
}

// TestRequireBearer_KeyLogIsRateLimitedPerReason: a JWKS outage answers every
// request with 503; the log gets one line per reason per minute, and the next
// line reports how many were held back. Mutation: share one throttle across
// reasons and the fetch-failure line is suppressed by the unknown-kid one.
func TestRequireBearer_KeyLogIsRateLimitedPerReason(t *testing.T) {
	logs := captureLogs(t)
	kl := newKeyUnavailableLog()
	unknown := jwksAuthConfig(jwksServer(t, "rotated-kid").URL, kl)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	unreachable := jwksAuthConfig(deadURL, kl)
	tok := mint(t, time.Hour)

	for i := 0; i < 5; i++ {
		if w := serveWithToken(unknown, tok); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status %d, want 503", i, w.Code)
		}
	}
	if w := serveWithToken(unreachable, tok); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	lines := keyLogLines(t, logs)
	if len(lines) != 2 {
		t.Fatalf("want one line per reason (2), got %d:\n%s", len(lines), logs)
	}
	if lines[0]["reason"] != keyReasonUnknownKid || lines[1]["reason"] != keyReasonJWKSUnavailable {
		t.Fatalf("reasons = %v, %v", lines[0]["reason"], lines[1]["reason"])
	}

	// After the interval the next line carries the count it held back.
	th := kl.byReason[keyReasonUnknownKid]
	if ok, n := th.allow(time.Now().Add(2 * time.Minute)); !ok || n != 4 {
		t.Fatalf("after the interval: allow=%v suppressed=%d, want true 4", ok, n)
	}
}

// TestRequireBearer_TokenFaultsDoNotLogKeyWarn is the control: a 401 (expired,
// garbage, wrong signature) is the caller's fault and gets no such line.
func TestRequireBearer_TokenFaultsDoNotLogKeyWarn(t *testing.T) {
	logs := captureLogs(t)
	a := testAuthConfig()
	for _, tok := range []string{mint(t, -time.Hour), "not.a.jwt"} {
		if w := serveWithToken(a, tok); w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", w.Code)
		}
	}
	if lines := keyLogLines(t, logs); len(lines) != 0 {
		t.Fatalf("a token fault must not log the key warning: %v", lines)
	}
}

func TestKeyUnavailableReason(t *testing.T) {
	for msg, want := range map[string]string{
		`token is unverifiable: error while executing keyfunc: token signing key unavailable: JWKS at http://authn/.well-known/jwks.json holds no key for kid "k2"`: keyReasonUnknownKid,
		`token signing key unavailable: JWKS at http://authn/.well-known/jwks.json holds 2 usable keys; token has no kid to select one`:                             keyReasonUnknownKid,
		`token signing key unavailable: fetching JWKS from http://authn/.well-known/jwks.json: dial tcp 10.0.0.1:8082: connect: connection refused`:                 keyReasonJWKSUnavailable,
		`token signing key unavailable: fetching JWKS from http://authn/.well-known/jwks.json: unexpected status 503 Service Unavailable`:                           keyReasonJWKSUnavailable,
		`token signing key unavailable: JWKS from http://authn/.well-known/jwks.json contains no usable RSA keys`:                                                   keyReasonJWKSUnavailable,
	} {
		if got := keyUnavailableReason(errString(msg)); got != want {
			t.Errorf("%q: reason %q, want %q", msg, got, want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }
