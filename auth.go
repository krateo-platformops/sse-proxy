// Authentication for the SSE proxy.
//
// The Krateo portal's RESTAction forwards the signed-in user's JWT
// (exportJwt: true) as `Authorization: Bearer <jwt>`. We validate that token
// the SAME way the snowplow service does: stateless HMAC (HS256) verification
// against a shared signing secret, using the krateo plumbing helper
// github.com/krateoplatformops/plumbing/jwtutil.Validate. No JWKS, no call-out
// to the authn service — authn signs the token with the same secret, so
// verification is purely local (snowplow internal/handlers/middleware:
// userconfig.go + refreshauth.go both call jwtutil.Validate).
//
// The signing secret comes from JWT_SIGN_KEY — the identical env/flag snowplow
// and authn read (snowplow main.go: flag "jwt-sign-key", env JWT_SIGN_KEY).
//
// Auth is OPT-IN: when JWT_SIGN_KEY is empty the middleware is a pass-through,
// preserving the proxy's previous unauthenticated behaviour for deployments
// that have not yet wired the secret. Set JWT_SIGN_KEY to enforce it.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/krateoplatformops/plumbing/http/response"
	"github.com/krateoplatformops/plumbing/jwtutil"
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
	// signingKey is the shared HMAC secret used to verify the JWT signature.
	// Empty disables auth (pass-through).
	signingKey string
	// sessionCookie is the cookie name the SSE/EventSource path reads the token
	// from when no Authorization header is present (matches snowplow's
	// REFRESH_SESSION_COOKIE, default "krateo-session").
	sessionCookie string
}

const (
	// envJWTSignKey is the shared HMAC signing secret. Identical name to
	// snowplow's and authn's flag/env so a single value validates everywhere.
	envJWTSignKey = "JWT_SIGN_KEY"

	// envSessionCookie / defaultSessionCookie mirror snowplow's RefreshAuth.
	envSessionCookie     = "REFRESH_SESSION_COOKIE"
	defaultSessionCookie = "krateo-session"
)

func loadAuthConfig() authConfig {
	return authConfig{
		signingKey:    getEnv(envJWTSignKey, ""),
		sessionCookie: getEnv(envSessionCookie, defaultSessionCookie),
	}
}

// enabled reports whether token validation is enforced.
func (a authConfig) enabled() bool { return a.signingKey != "" }

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

// validate verifies the token exactly as snowplow does (jwtutil.Validate:
// HS256 against signingKey, 5s leeway, exp enforced; iss/aud not checked). The
// returned UserInfo carries the username + groups from the verified claims.
func (a authConfig) validate(token string) (jwtutil.UserInfo, error) {
	return jwtutil.Validate(a.signingKey, token)
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
			// err is jwtutil.ErrTokenExpired / ErrTokenInvalid — safe to surface
			// (it contains no token material); both map to 401, as in snowplow.
			_ = response.Unauthorized(w, err)
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
			_ = response.Unauthorized(w, err)
			return
		}
		next(w, r.WithContext(withUserInfo(r.Context(), ui)))
	}
}

// logStatus prints a one-line summary of the auth posture at startup, without
// ever printing the secret itself.
func (a authConfig) logStatus() {
	if a.enabled() {
		slogInfo("sse-proxy", "auth ENABLED (HS256); SSE token via Authorization header, cookie, or ?access_token=/?token=",
			slog.String("sign_key_env", envJWTSignKey),
			slog.String("session_cookie", a.sessionCookie),
		)
	} else {
		slogInfo("sse-proxy", "auth DISABLED — all endpoints are open; set the sign key env to enforce JWT validation",
			slog.String("sign_key_env", envJWTSignKey),
		)
	}
}
