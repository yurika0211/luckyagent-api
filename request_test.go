package transport

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNoKeyExceptionRequiresDirectSameOriginLoopback(t *testing.T) {
	for _, tc := range []struct {
		name, host, origin, forwarded, value string
		want                                 int
	}{
		{name: "native loopback", host: "localhost:9090", want: 200},
		{name: "same origin", host: "localhost:9090", origin: "http://localhost:9090", want: 200},
		{name: "default port", host: "localhost:80", origin: "http://localhost", want: 200},
		{name: "IPv6 host", host: "[::1]:9090", origin: "http://[::1]:9090", want: 200},
		{name: "foreign host", host: "attacker.example:9090", want: 401},
		{name: "foreign origin", host: "localhost:9090", origin: "http://attacker.example", want: 401},
		{name: "different port", host: "localhost:9090", origin: "http://localhost:3000", want: 401},
		{name: "null origin", host: "localhost:9090", origin: "null", want: 401},
		{name: "origin with path", host: "localhost:9090", origin: "http://localhost:9090/path", want: 401},
		{name: "proxy XFF", host: "localhost:9090", forwarded: "X-Forwarded-For", value: "198.51.100.3", want: 401},
		{name: "proxy Forwarded", host: "localhost:9090", forwarded: "Forwarded", value: "for=198.51.100.3", want: 401},
		{name: "proxy real IP", host: "localhost:9090", forwarded: "X-Real-IP", value: "198.51.100.3", want: 401},
		{name: "empty forwarding header", host: "localhost:9090", forwarded: "Forwarded", want: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := NewMiddleware(DefaultConfig(), nil, nil, nil)
			r := httptest.NewRequest(http.MethodPost, "/api/v1/chat", nil)
			r.Host, r.RemoteAddr = tc.host, "127.0.0.1:1234"
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.forwarded != "" {
				r.Header.Set(tc.forwarded, tc.value)
			}
			w := httptest.NewRecorder()
			policy.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func protectedRequest(h http.Handler, method, path, key, remote string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.RemoteAddr = remote
	r.Header.Set("X-API-Key", key)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestProtectLimitsAuthenticationAttemptsBeforeAuthentication(t *testing.T) {
	policy := NewMiddleware(Config{APIKeys: []string{"valid"}, RateLimit: 2}, nil, nil, nil)
	h := policy.Protect(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	for i := 0; i < 61; i++ {
		w := protectedRequest(h, http.MethodPost, "/api/v1/chat", "private-wrong-key", "192.0.2.1:1000")
		want := http.StatusForbidden
		if i == 60 {
			want = http.StatusTooManyRequests
		}
		if w.Code != want {
			t.Fatalf("attempt %d status=%d, want %d", i+1, w.Code, want)
		}
		var response ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.ErrorCode == "" || response.RequestID == "" || strings.Contains(w.Body.String(), "private-wrong-key") {
			t.Fatalf("unsafe or incomplete error response: %s", w.Body.String())
		}
	}
}

func TestProtectSeparatesCredentialAndPublicProbeQuotas(t *testing.T) {
	policy := NewMiddleware(Config{APIKeys: []string{"a", "b", "c"}, RateLimit: 2}, nil, nil, nil)
	h := policy.Protect(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	for _, tc := range []struct {
		method, path, key, remote string
		want                      int
	}{
		{"POST", "/api/v1/chat", "a", "127.0.0.1:1000", 200},
		{"POST", "/api/v1/chat", "a", "127.0.0.1:1001", 200},
		{"POST", "/api/v1/chat", "a", "192.0.2.2:1000", 429},
		{"POST", "/api/v1/chat", "b", "127.0.0.1:1002", 200},
		{"GET", "/api/v1/health/live", "", "127.0.0.1:1003", 200},
		{"GET", "/api/v1/health/ready", "", "127.0.0.1:1004", 200},
		{"GET", "/api/v1/health/live", "", "127.0.0.1:1005", 429},
		{"POST", "/api/v1/chat", "b", "127.0.0.1:1006", 200},
		{"POST", "/api/v1/chat", "c", "127.0.0.1:1007", 200},
		{"GET", "/api/v1/health/detail", "", "192.0.2.1:1000", 401},
		{"GET", "/api/v1/health/anything", "", "192.0.2.1:1000", 401},
	} {
		w := protectedRequest(h, tc.method, tc.path, tc.key, tc.remote)
		if w.Code != tc.want {
			t.Fatalf("%s %s key=%q status=%d, want %d", tc.method, tc.path, tc.key, w.Code, tc.want)
		}
	}
	for bucket := range policy.rateLimiter.clients {
		if bucket == "credential:a" || bucket == "credential:b" {
			t.Fatalf("plaintext credential retained: %s", bucket)
		}
	}
}

func TestProtectBoundsAndValidatesRequestBeforeHandler(t *testing.T) {
	large := `{"message":"` + strings.Repeat("x", 4<<20) + `"}`
	for _, tc := range []struct {
		name, path, contentType, body string
		chunked                       bool
		want                          int
	}{
		{name: "large JSON", path: "/api/v1/chat", contentType: "application/json", body: large, want: 413},
		{name: "large chunked JSON", path: "/api/v1/chat", body: large, chunked: true, want: 413},
		{name: "non JSON", path: "/api/v1/chat", contentType: "text/plain", body: `{}`, want: 415},
		{name: "missing content type", path: "/api/v1/chat", body: `{}`, want: 200},
		{name: "JSON charset", path: "/api/v1/chat", contentType: "application/json; charset=utf-8", body: `{}`, want: 200},
		{name: "JSON suffix", path: "/api/v1/chat", contentType: "application/merge-patch+json", body: `{}`, want: 200},
		{name: "trailing JSON", path: "/api/v1/chat", body: `{} {}`, want: 400},
		{name: "malformed JSON", path: "/api/v1/chat", body: `{`, want: 400},
		{name: "multipart attachment", path: "/api/v1/uploads", contentType: "multipart/form-data; boundary=example", body: strings.Repeat("x", 3<<20), want: 200},
		{name: "multipart skill", path: "/api/v1/skills/install", contentType: "multipart/form-data; boundary=example", body: strings.Repeat("x", 3<<20), want: 200},
		{name: "multipart wrong route", path: "/api/v1/chat", contentType: "multipart/form-data; boundary=example", body: "abc", want: 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			policy := NewMiddleware(Config{APIKeys: []string{"valid"}}, nil, nil, nil)
			h := policy.Protect(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(200) }))
			r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			r.Header.Set("X-API-Key", "valid")
			if tc.chunked {
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want || called != (tc.want == 200) {
				t.Fatalf("status=%d, want %d; handler called=%v: %s", w.Code, tc.want, called, w.Body.String())
			}
		})
	}
}

func TestDecodeJSONRequiresOneBoundedValue(t *testing.T) {
	for _, tc := range []struct {
		body, contentType string
		limit             int64
		want              int
	}{
		{`{"value":1}`, "", 64, 200},
		{`{"value":1} {}`, "application/json", 64, 400},
		{`{"value":1} xyz`, "application/json", 64, 400},
		{`{"value":1}`, "text/plain", 64, 415},
		{`{"value":1}` + strings.Repeat(" ", 64), "", 32, 413},
	} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", tc.contentType)
		w := httptest.NewRecorder()
		var value map[string]any
		if err := DecodeJSON(w, r, &value, tc.limit); err != nil {
			SendRequestError(w, err)
		}
		if w.Code != tc.want {
			t.Fatalf("body=%q status=%d, want %d: %s", tc.body, w.Code, tc.want, w.Body.String())
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	var dst any
	if err := DecodeJSON(httptest.NewRecorder(), r, &dst, 64); !errors.Is(err, io.EOF) {
		t.Fatalf("optional empty body: %v", err)
	}
}
