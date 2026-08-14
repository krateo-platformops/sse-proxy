// Authentication for the SSE proxy.
//
// The Krateo portal's RESTAction forwards the signed-in user's JWT
// (exportJwt: true) as `Authorization: Bearer <jwt>`. We validate that token
// the SAME way the snowplow service does: stateless RS256 signature
// verification against authn's PUBLIC key, resolved by the token's "kid" from
// authn's JWKS endpoint (GET /.well-known/jwks.json) via the krateo plumbing
// helper github.com/krateo-platformops/plumbing/jwtutil.ValidateWithKeySource.
// authn signs with its RSA private key and publishes the matching public key
// set, so this proxy holds NO shared secret and key rotation needs no redeploy
// here (snowplow internal/handlers/middleware userconfig.go + refreshauth.go
// call the same ValidateWithKeySource). Only RS256 is accepted — a token
// offering HMAC is rejected before the key is consulted, so algorithm confusion
// is impossible.
//
// The JWKS URL is derived from authn's base URL — URL_AUTHN (the identical env
// snowplow reads), joined with /.well-known/jwks.json — or set directly via
// JWT_JWKS_URL.
//
// Auth ENFORCES by default: URL_AUTHN carries a cluster-internal default so the
// key source is always built. To run the proxy open (e.g. a local dev harness),
// set BOTH URL_AUTHN and JWT_JWKS_URL empty — the middleware then pass-throughs
// (logged loudly at startup).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// ---------------------------------------------------------------------------
// Verified caller identity in the request context.
//
// The auth middlewares stash the jwtutil.UserInfo (username + groups) from the
// VERIFIED token into the request context so downstream handlers (the RBAC
// namespace scoper, see rbac.go) can derive the caller's authorization scope.
// Handlers must only ever trust identity coming from this context key — it is
// set exclusively after signature validation.
// ---------------------------------------------------------------------------

type userInfoCtxKey struct{}

// withUserInfo returns a context carrying the verified caller identity.
func withUserInfo(ctx context.Context, u jwtutil.UserInfo) context.Context {
	return context.WithValue(ctx, userInfoCtxKey{}, u)
}

// userInfoFrom extracts the verified caller identity, if any.
func userInfoFrom(ctx context.Context) (jwtutil.UserInfo, bool) {
	u, ok := ctx.Value(userInfoCtxKey{}).(jwtutil.UserInfo)
	return u, ok
}

// authConfig holds the auth-related runtime configuration.
type authConfig struct {
	// keys resolves authn's RSA public keys (by "kid") from authn's JWKS
	// endpoint. nil disables auth (pass-through).
	keys jwtutil.KeySource
	// jwksURL is the resolved JWKS document URL, kept for the startup log line.
	jwksURL string
	// sessionCookie is the cookie name the SSE/EventSource path reads the token
	// from when no Authorization header is present (matches snowplow's
	// REFRESH_SESSION_COOKIE, default "krateo-session").
	sessionCookie string
}

const (
	// envURLAuthn is authn's base URL; the JWKS document is derived from it as
	// <URL_AUTHN>/.well-known/jwks.json. Identical env to snowplow's URL_AUTHN,
	// so one value configures every verifier.
	envURLAuthn     = "URL_AUTHN"
	defaultURLAuthn = "http://authn.krateo-system.svc.cluster.local:8082"

	// envJWKSURL optionally sets the full JWKS document URL directly, overriding
	// the URL_AUTHN-derived one.
	envJWKSURL = "JWT_JWKS_URL"

	// JWKS cache/refresh/timeout knobs — same env names + defaults as snowplow.
	envJWKSCacheTTL       = "JWT_JWKS_CACHE_TTL"
	envJWKSMinRefresh     = "JWT_JWKS_MIN_REFRESH_INTERVAL"
	envJWKSRequestTimeout = "JWT_JWKS_REQUEST_TIMEOUT"
	defaultJWKSCacheTTL   = 5 * time.Minute
	defaultJWKSMinRefresh = 30 * time.Second
	defaultJWKSTimeout    = 5 * time.Second

	// envSessionCookie / defaultSessionCookie mirror snowplow's RefreshAuth.
	envSessionCookie     = "REFRESH_SESSION_COOKIE"
	defaultSessionCookie = "krateo-session"
)

func loadAuthConfig() authConfig {
	// Prefer an explicit JWKS URL; otherwise derive it from authn's base URL.
	jwksURL := strings.TrimSpace(getEnv(envJWKSURL, ""))
	if jwksURL == "" {
		if base := strings.TrimSpace(getEnv(envURLAuthn, defaultURLAuthn)); base != "" {
			jwksURL = jwtutil.JWKSURL(base)
		}
	}
	var keys jwtutil.KeySource
	if jwksURL != "" {
		keys = jwtutil.NewJWKSKeySource(jwksURL,
			jwtutil.WithJWKSCacheTTL(getEnvDuration(envJWKSCacheTTL, defaultJWKSCacheTTL)),
			jwtutil.WithJWKSMinRefreshInterval(getEnvDuration(envJWKSMinRefresh, defaultJWKSMinRefresh)),
			jwtutil.WithJWKSRequestTimeout(getEnvDuration(envJWKSRequestTimeout, defaultJWKSTimeout)))
	}
	return authConfig{
		keys:          keys,
		jwksURL:       jwksURL,
		sessionCookie: getEnv(envSessionCookie, defaultSessionCookie),
	}
}

// getEnvDuration reads a time.Duration from the environment, falling back to the
// default when unset or unparseable.
func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(getEnv(key, "")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// enabled reports whether token validation is enforced.
func (a authConfig) enabled() bool { return a.keys != nil }

// bearerFromHeader extracts the token from an `Authorization: Bearer <jwt>`
// header. Parsing rule is identical to snowplow (split on the first space into
// two parts; require exactly two and a case-insensitive "bearer" scheme).
func bearerFromHeader(r *http.Request) (string, bool) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", false
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") && parts[1] != "" {
		return parts[1], true
	}
	return "", false
}

// tokenFromRequestHeaderOrCookie returns the token from, in order:
//
//	(a) the Authorization: Bearer header, then
//	(b) the session cookie.
//
// This is the snowplow UserConfig/RefreshAuth convention (header first so curl
// and non-browser clients work, cookie second for the browser).
func (a authConfig) tokenFromRequestHeaderOrCookie(r *http.Request) (string, bool) {
	if tok, ok := bearerFromHeader(r); ok {
		return tok, true
	}
	if a.sessionCookie != "" {
		if ck, err := r.Cookie(a.sessionCookie); err == nil && ck.Value != "" {
			return ck.Value, true
		}
	}
	return "", false
}

// tokenFromRequestSSE returns the token for the EventSource/SSE path. It tries
// the header and cookie first (snowplow's convention) and then falls back to a
// query parameter (`access_token`, then `token`).
//
// The query-param fallback exists ONLY because the browser EventSource API
// cannot set request headers and the portal opens the stream with
// withCredentials:false (so the session cookie is not sent cross-origin).
// snowplow's own /refreshes deliberately forbids token-in-URL to avoid leaking
// it via access logs/Referer; we accept it here as the documented EventSource
// fallback, so callers MUST keep this token out of logs (this proxy never logs
// request URLs or query strings).
func (a authConfig) tokenFromRequestSSE(r *http.Request) (string, bool) {
	if tok, ok := a.tokenFromRequestHeaderOrCookie(r); ok {
		return tok, true
	}
	q := r.URL.Query()
	for _, key := range []string{"access_token", "token"} {
		if v := q.Get(key); v != "" {
			return v, true
		}
	}
	return "", false
}

// validate verifies the token exactly as snowplow does: stateless RS256
// signature verification, resolving authn's public key by the token's "kid"
// through the JWKS key source (5s leeway, exp enforced; iss/aud not checked).
// The returned UserInfo carries the username + groups from the verified claims.
func (a authConfig) validate(token string) (jwtutil.UserInfo, error) {
	return jwtutil.ValidateWithKeySource(a.keys, token)
}

// writeAuthError maps a validation failure to the right status. A token fault
// (expired/invalid) is 401. An inability to obtain authn's key (JWKS
// unreachable / unknown kid) is a transient 503 that says nothing about the
// token — mirroring snowplow, so an authn outage does not masquerade as a bad
// credential.
func writeAuthError(w http.ResponseWriter, err error) {
	if errors.Is(err, jwtutil.ErrKeyUnavailable) {
		_ = response.ServiceUnavailable(w, err)
		return
	}
	_ = response.Unauthorized(w, err)
}

// requireBearer wraps a handler with Authorization: Bearer (or cookie)
// validation — used for the plain HTTP /events endpoint. A no-op when auth is
// disabled.
func (a authConfig) requireBearer(next http.HandlerFunc) http.HandlerFunc {
	if !a.enabled() {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := a.tokenFromRequestHeaderOrCookie(r)
		if !ok {
			// Do not echo any token material in the error.
			_ = response.Unauthorized(w, fmt.Errorf("missing or malformed Authorization header"))
			return
		}
		ui, err := a.validate(token)
		if err != nil {
			// 401 for a token fault, 503 for an unavailable key — both carry no
			// token material.
			writeAuthError(w, err)
			return
		}
		next(w, r.WithContext(withUserInfo(r.Context(), ui)))
	}
}

// requireSSEToken wraps the SSE handler, accepting the token via header,
// cookie, or query param (EventSource fallback). A no-op when auth is disabled.
func (a authConfig) requireSSEToken(next http.HandlerFunc) http.HandlerFunc {
	if !a.enabled() {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := a.tokenFromRequestSSE(r)
		if !ok {
			_ = response.Unauthorized(w, fmt.Errorf("missing credentials: no bearer header, session cookie, or token query param"))
			return
		}
		ui, err := a.validate(token)
		if err != nil {
			writeAuthError(w, err)
			return
		}
		next(w, r.WithContext(withUserInfo(r.Context(), ui)))
	}
}

// logStatus prints a one-line summary of the auth posture at startup, without
// ever printing key material.
func (a authConfig) logStatus() {
	if a.enabled() {
		slogInfo("sse-proxy", "auth ENABLED (RS256/JWKS); SSE token via Authorization header, cookie, or ?access_token=/?token=",
			slog.String("jwks_url", a.jwksURL),
			slog.String("session_cookie", a.sessionCookie),
		)
	} else {
		slogInfo("sse-proxy", "auth DISABLED — all endpoints are open; set URL_AUTHN or JWT_JWKS_URL to enforce RS256/JWKS validation",
			slog.String("url_authn_env", envURLAuthn),
			slog.String("jwks_url_env", envJWKSURL),
		)
	}
}
