// Package transport owns the HTTP, SSE, WebSocket and gRPC wire boundary.
// Application operations are supplied by the embedding runtime.
package transport

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	logger "log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config API Server 配置
type Config struct {
	Addr        string   `yaml:"addr,omitempty"`         // 监听地址，默认 :9090
	APIKeys     []string `yaml:"api_keys,omitempty"`     // API Key 白名单（空=不鉴权）
	EnableCORS  bool     `yaml:"enable_cors,omitempty"`  // 启用 CORS，默认 true
	CORSOrigins []string `yaml:"cors_origins,omitempty"` // CORS 允许的源
	RateLimit   int      `yaml:"rate_limit,omitempty"`   // 每分钟请求限制，默认 60
	MetricsAddr string   `yaml:"metrics_addr,omitempty"` // Prometheus metrics 独立端口（空=复用主端口）
	LogLevel    string   `yaml:"log_level,omitempty"`    // 日志级别: debug, info, warn, error
	LogFormat   string   `yaml:"log_format,omitempty"`   // 日志格式: json, text
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Addr:        "127.0.0.1:9090", // 默认仅本地访问
		EnableCORS:  false,            // 默认关闭 CORS
		CORSOrigins: []string{},       // 空白名单 = 不允许跨域
		RateLimit:   60,
		LogLevel:    "info",
		LogFormat:   "text",
	}
}

// ErrorResponse 错误响应
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *responseRecorder) ReadFrom(src io.Reader) (int64, error) {
	rf, ok := r.ResponseWriter.(io.ReaderFrom)
	if !ok {
		return io.Copy(r, src)
	}
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}
	n, err := rf.ReadFrom(src)
	r.bytes += int(n)
	return n, err
}

func (r *responseRecorder) StatusCode() int {
	if r.statusCode == 0 {
		return http.StatusOK
	}
	return r.statusCode
}

func (r *responseRecorder) Flush() {
	_ = r.FlushError()
}

func (r *responseRecorder) FlushError() error {
	return http.NewResponseController(r.ResponseWriter).Flush()
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return h.Hijack()
}

func (r *responseRecorder) Push(target string, opts *http.PushOptions) error {
	p, ok := r.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return p.Push(target, opts)
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// PublicServerURL turns a bound listener address into a URL a person can open.
// Wildcard binds stay on localhost instead of printing "localhost127.0.0.1:9090".
func PublicServerURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.HasPrefix(addr, ":") {
			return "http://localhost" + addr
		}
		return "http://" + addr
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "localhost"
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// corsMiddleware CORS 中间件
func (s *Middleware) CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.config.EnableCORS {
			next.ServeHTTP(w, r)
			return
		}

		origin := r.Header.Get("Origin")
		allowed := false

		if len(s.config.CORSOrigins) == 0 {
			// 无配置 = 不允许任何跨域（安全默认值）
			allowed = false
		} else {
			for _, o := range s.config.CORSOrigins {
				if o == "*" || o == origin {
					allowed = true
					break
				}
			}
		}

		if allowed {
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			} else {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
			// DELETE is required by handleAgentsDeregister and by the skill uninstall
			// alias; without it those endpoints fail preflight in a browser.
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}

		if r.Method == http.MethodOptions {
			if allowed {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}

// authMiddleware API Key 认证中间件
func (s *Middleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 健康检查不需要认证
		if strings.HasPrefix(r.URL.Path, "/api/v1/health/") || r.URL.Path == "/" {
			next.ServeHTTP(w, r)
			return
		}

		// 从 Header 获取 API Key（不再支持 query string，防止日志泄露）
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.Header.Get("Authorization")
			if strings.HasPrefix(apiKey, "Bearer ") {
				apiKey = strings.TrimPrefix(apiKey, "Bearer ")
			}
		}

		// 无配置 API Key 则仅允许本地访问。扫码签发的临时 key 仍可从局域网进入。
		if len(s.config.APIKeys) == 0 {
			if s.pairingKeyValid(apiKey) {
				next.ServeHTTP(w, r)
				return
			}
			ip := RemoteIP(r.RemoteAddr)
			if ip != "127.0.0.1" && ip != "::1" && ip != "localhost" {
				logger.Warn("auth rejected non-local request without api keys",
					"path", r.URL.Path,
					"method", r.Method,
					"remote_addr", r.RemoteAddr,
				)
				SendError(w, "api key required (no keys configured, localhost only)", http.StatusUnauthorized,
					"configure api_keys in server config, scan lh qr, or access from localhost")
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		if apiKey == "" {
			logger.Warn("auth missing api key",
				"path", r.URL.Path,
				"method", r.Method,
				"remote_addr", r.RemoteAddr,
			)
			SendError(w, "api key required", http.StatusUnauthorized, "provide X-API-Key header or Authorization: Bearer <key>")
			return
		}

		// 常量时间比较，防止 timing attack
		valid := s.pairingKeyValid(apiKey)
		for _, k := range s.config.APIKeys {
			if subtle.ConstantTimeCompare([]byte(k), []byte(apiKey)) == 1 {
				valid = true
				break
			}
		}

		if !valid {
			logger.Warn("auth invalid api key",
				"path", r.URL.Path,
				"method", r.Method,
				"remote_addr", r.RemoteAddr,
			)
			SendError(w, "invalid api key", http.StatusForbidden, "")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware 限流中间件
func (s *Middleware) RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := RemoteIP(r.RemoteAddr)
		if !s.rateLimiter.Allow(ip) {
			logger.Warn("rate limit exceeded",
				"path", r.URL.Path,
				"method", r.Method,
				"remote_addr", r.RemoteAddr,
				"limit_per_min", s.config.RateLimit,
			)
			SendError(w, "rate limit exceeded", http.StatusTooManyRequests,
				fmt.Sprintf("limit: %d req/min", s.config.RateLimit))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RemoteIP strips the transport port from RemoteAddr and handles both
// bracketed and unbracketed IPv6 addresses.
func RemoteIP(remoteAddr string) string {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(remoteAddr, "[]")
}

// loggingMiddleware 日志中间件
func (s *Middleware) Log(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		clientIP := r.RemoteAddr
		if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
			parts := strings.Split(xff, ",")
			if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
				clientIP = strings.TrimSpace(parts[0])
			}
		} else if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
			clientIP = xrip
		} else if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			clientIP = host
		}

		fields := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.StatusCode(),
			"duration_ms", duration.Milliseconds(),
			"bytes", rec.bytes,
			"client_ip", clientIP,
		}
		if ua := strings.TrimSpace(r.UserAgent()); ua != "" {
			fields = append(fields, "user_agent", ua)
		}

		switch {
		case rec.StatusCode() >= http.StatusInternalServerError:
			logger.Error("http request completed", fields...)
		case rec.StatusCode() >= http.StatusBadRequest:
			logger.Warn("http request completed", fields...)
		default:
			logger.Info("http request completed", fields...)
		}
	})
}

// recoveryMiddleware 恢复中间件
func (s *Middleware) Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				if s.onPanic != nil {
					s.onPanic()
				}
				// 内部错误详情只写日志，不返回给客户端
				logger.Error("panic recovered",
					"path", r.URL.Path,
					"method", r.Method,
					"panic", err,
				)
				SendError(w, "internal server error", http.StatusInternalServerError, "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type Limiter struct {
	mu       sync.RWMutex
	limit    int
	clients  map[string]*clientBucket
	stopCh   chan struct{}
	stopOnce sync.Once
}

type clientBucket struct {
	count   int
	resetAt time.Time
}

func NewLimiter(limit int) *Limiter {
	rl := &Limiter{
		limit:   limit,
		clients: make(map[string]*clientBucket),
		stopCh:  make(chan struct{}),
	}

	// 后台清理过期桶
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rl.cleanup()
			case <-rl.stopCh:
				return
			}
		}
	}()

	return rl
}

func (rl *Limiter) Stop() {
	if rl == nil || rl.stopCh == nil {
		return
	}
	rl.stopOnce.Do(func() { close(rl.stopCh) })
}

func (rl *Limiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	bucket, ok := rl.clients[ip]
	if !ok || now.After(bucket.resetAt) {
		rl.clients[ip] = &clientBucket{
			count:   1,
			resetAt: now.Add(time.Minute),
		}
		return true
	}

	bucket.count++
	return bucket.count <= rl.limit
}

func (rl *Limiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for ip, bucket := range rl.clients {
		if now.After(bucket.resetAt) {
			delete(rl.clients, ip)
		}
	}
}

// Middleware holds transport policy without depending on the host runtime.
type Middleware struct {
	config          Config
	rateLimiter     *Limiter
	pairingKeyValid func(string) bool
	onPanic         func()
}

// NewMiddleware accepts optional host-owned pairing and panic-accounting hooks.
func NewMiddleware(config Config, limiter *Limiter, pairingKeyValid func(string) bool, onPanic func()) *Middleware {
	if pairingKeyValid == nil {
		pairingKeyValid = func(string) bool { return false }
	}
	return &Middleware{config: config, rateLimiter: limiter, pairingKeyValid: pairingKeyValid, onPanic: onPanic}
}

func SendJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func SendError(w http.ResponseWriter, message string, code int, details string) {
	SendJSON(w, code, ErrorResponse{Error: message, Code: code, Details: details})
}
func DispatchMethod(w http.ResponseWriter, r *http.Request, handlers map[string]func()) bool {
	handler, ok := handlers[r.Method]
	if !ok {
		SendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return false
	}
	handler()
	return true
}
func SendSSEError(w io.Writer, flusher http.Flusher, message string) {
	data, _ := json.Marshal(map[string]any{"type": "error", "error": message})
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
}

// NewHTTPServer preserves the existing streaming-friendly listener timeouts.
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 120 * time.Second}
}
