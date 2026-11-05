package main

import "net/http"

// withCORS makes every response readable cross-origin and answers CORS preflights
// before authentication.
//
// The portal streams with fetch and an Authorization header, which a browser sends
// only after a preflight, and a preflight never carries credentials. Without this a
// cross-origin portal gets a 401 on the preflight and the stream never opens. Where
// a gateway answers preflights itself the request never reaches this code.
//
// The origin is "*". That is safe only while Access-Control-Allow-Credentials is never
// sent: this proxy also accepts the session cookie, and a browser refuses to let a
// credentialed request read a "*" response. Never reflect the Origin or allow credentials
// here, or cookie-authenticated events become readable cross-site.
//
// traceparent and tracestate are allowed because the portal can send them with these
// requests when browser tracing is on, and main wraps the router to extract them.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, traceparent, tracestate")
			w.Header().Set("Access-Control-Max-Age", "300")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
