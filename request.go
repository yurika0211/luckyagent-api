package transport

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultJSONBodyLimit applies to client JSON, including inline attachments.
	DefaultJSONBodyLimit int64 = 2 << 20
	// Multipart upload handlers may enforce a smaller limit of their own.
	DefaultUploadBodyLimit int64 = 64 << 20
)

// RequestError contains only a public message, never the submitted body or key.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

func statusErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request"
	case http.StatusUnauthorized:
		return "authentication_required"
	case http.StatusForbidden:
		return "access_denied"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusConflict:
		return "conflict"
	case http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case http.StatusUnsupportedMediaType:
		return "unsupported_media_type"
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusServiceUnavailable:
		return "unavailable"
	default:
		if status >= 500 {
			return "internal_error"
		}
		return "request_failed"
	}
}

// SendRequestError maps decoding failures into the common JSON error envelope.
func SendRequestError(w http.ResponseWriter, err error) {
	var requestError *RequestError
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &requestError):
		SendError(w, requestError.Message, requestError.Status, "")
	case errors.As(err, &tooLarge):
		SendError(w, "request body too large", http.StatusRequestEntityTooLarge, "")
	default:
		SendError(w, "invalid JSON request body", http.StatusBadRequest, "")
	}
}

func jsonMediaType(r *http.Request) error {
	value := strings.TrimSpace(r.Header.Get("Content-Type"))
	if value == "" {
		// Existing native clients and in-process callers omit this header.
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || (mediaType != "application/json" && !(strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json"))) {
		return &RequestError{Status: http.StatusUnsupportedMediaType, Message: "Content-Type must be application/json"}
	}
	return nil
}

// DecodeJSON reads exactly one JSON value within maxBytes. Empty bodies return
// io.EOF so optional-body endpoints can retain their existing behavior.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	if err := jsonMediaType(r); err != nil {
		return err
	}
	if r.Body == nil {
		return io.EOF
	}
	if maxBytes <= 0 {
		maxBytes = DefaultJSONBodyLimit
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return &RequestError{Status: http.StatusBadRequest, Message: "request body must contain one JSON value"}
	}
	return nil
}

// Protect is the complete HTTP policy. Hosts should wrap it with logging and
// their optional CORS middleware, without adding Authenticate/RateLimit again.
// Independent, generous per-peer admission limits protect authentication; the
// business limit is per credential, with public probes in a separate IP bucket.
func (s *Middleware) Protect(next http.Handler) http.Handler {
	handler := s.Authenticate(s.RateLimit(s.requestBoundary(next)))
	handler = s.admit(handler)
	handler = s.Recover(handler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id [16]byte
		if _, err := rand.Read(id[:]); err == nil {
			w.Header().Set("X-Request-ID", hex.EncodeToString(id[:]))
		}
		handler.ServeHTTP(w, r)
	})
}

func (s *Middleware) admit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := "admission:" + RemoteIP(r.RemoteAddr)
		if publicHealthRequest(r) {
			key = "probe-admission:" + RemoteIP(r.RemoteAddr)
		}
		limit := s.config.RateLimit
		maxInt := int(^uint(0) >> 1)
		if limit > maxInt/10 {
			limit = maxInt
		} else {
			limit *= 10
		}
		if limit < 60 {
			limit = 60
		}
		if !s.rateLimiter.allowWithLimit(key, limit) {
			w.Header().Set("Retry-After", "60")
			SendError(w, "request admission rate exceeded", http.StatusTooManyRequests, "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type replayRequestBody struct {
	*bytes.Reader
	io.Closer
}

func (s *Middleware) requestBoundary(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType == "multipart/form-data" && (r.URL.Path == "/api/v1/uploads" || r.URL.Path == "/api/v1/skills/install") {
			if r.ContentLength > DefaultUploadBodyLimit {
				SendError(w, "request body too large", http.StatusRequestEntityTooLarge, "")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, DefaultUploadBodyLimit)
			next.ServeHTTP(w, r)
			return
		}
		if err := jsonMediaType(r); err != nil {
			SendRequestError(w, err)
			return
		}
		if r.ContentLength > DefaultJSONBodyLimit {
			SendError(w, "request body too large", http.StatusRequestEntityTooLarge, "")
			return
		}
		original := r.Body
		body, err := io.ReadAll(io.LimitReader(original, DefaultJSONBodyLimit+1))
		if err != nil {
			SendRequestError(w, err)
			return
		}
		if int64(len(body)) > DefaultJSONBodyLimit {
			SendError(w, "request body too large", http.StatusRequestEntityTooLarge, "")
			return
		}
		if len(bytes.TrimSpace(body)) > 0 && !json.Valid(body) {
			SendError(w, "invalid JSON request body", http.StatusBadRequest, "")
			return
		}
		r.Body = &replayRequestBody{Reader: bytes.NewReader(body), Closer: original}
		next.ServeHTTP(w, r)
	})
}

// pruneLocked bounds memory even for a host that supplies no cleanup goroutine.
func (rl *Limiter) pruneLocked(now time.Time) {
	if now.Before(rl.nextPrune) {
		return
	}
	rl.nextPrune = now.Add(time.Minute)
	for key, bucket := range rl.clients {
		if now.After(bucket.resetAt) {
			delete(rl.clients, key)
		}
	}
}
