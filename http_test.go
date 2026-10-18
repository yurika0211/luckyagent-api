package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthenticationPreservesHeaderOnlyAndLocalOnlyPolicies(t *testing.T) {
	for _, tc := range []struct {
		name, path, remote, key string
		keys                    []string
		want                    int
	}{
		{"local without keys", "/api/v1/chat", "127.0.0.1:1000", "", nil, 200},
		{"remote without keys", "/api/v1/chat", "192.0.2.1:1000", "", nil, 401},
		{"query key ignored", "/api/v1/chat?api_key=valid", "192.0.2.1:1000", "", []string{"valid"}, 401},
		{"wrong key", "/api/v1/chat", "192.0.2.1:1000", "wrong", []string{"valid"}, 403},
		{"valid header", "/api/v1/chat", "192.0.2.1:1000", "valid", []string{"valid"}, 200},
		{"health bypass", "/api/v1/health/live", "192.0.2.1:1000", "", []string{"valid"}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := NewMiddleware(Config{APIKeys: tc.keys}, nil, nil, nil)
			handler := policy.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.RemoteAddr = tc.remote
			req.Host = "localhost"
			req.Header.Set("X-API-Key", tc.key)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestPairingHookAndCORSAreHostIndependent(t *testing.T) {
	policy := NewMiddleware(Config{EnableCORS: true, CORSOrigins: []string{"https://app.example"}}, nil, func(key string) bool { return key == "paired" }, nil)
	leaf := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	req := httptest.NewRequest(http.MethodGet, "/api/v1/chat", nil)
	req.RemoteAddr = "192.0.2.1:1000"
	req.Header.Set("X-API-Key", "paired")
	w := httptest.NewRecorder()
	policy.Authenticate(leaf).ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("pairing status = %d", w.Code)
	}
	for _, origin := range []string{"https://app.example", "https://other.example"} {
		req := httptest.NewRequest(http.MethodOptions, "/api/v1/chat", nil)
		req.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		policy.CORS(leaf).ServeHTTP(w, req)
		want := 403
		if origin == "https://app.example" {
			want = 204
		}
		if w.Code != want {
			t.Fatalf("origin %s: status = %d, want %d", origin, w.Code, want)
		}
	}
}

func TestRecoveryAndSSEFlushThroughLogging(t *testing.T) {
	panics := 0
	policy := NewMiddleware(Config{}, nil, nil, func() { panics++ })
	w := httptest.NewRecorder()
	policy.Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("private detail") })).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	var response ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Code != 500 || panics != 1 || strings.Contains(w.Body.String(), "private detail") {
		t.Fatalf("unexpected panic response: %s; count=%d; error=%v", w.Body.String(), panics, err)
	}
	w = httptest.NewRecorder()
	policy.Log(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		SendSSEError(w, w.(http.Flusher), "stream interrupted")
	})).ServeHTTP(w, httptest.NewRequest("GET", "/stream", nil))
	if !w.Flushed || !strings.Contains(w.Body.String(), `"type":"error"`) {
		t.Fatalf("SSE did not flush through middleware: %s", w.Body.String())
	}
}
