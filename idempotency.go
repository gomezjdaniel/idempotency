package idempotency

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"
)

const (
	RecoveryPointStart = "start"
)

const (
	DefaultIdempotencyKeyHeader = "Idempotency-Key"
	DefaultLockDuration         = 10 * time.Second
)

type Locker interface {
	Lock(key string) bool
	Unlock(key string)
}

type IdempotencyKey struct {
	Key                string
	RecoveryPoint      string
	RequestMethod      string
	RequestURLPath     string
	RequestURLRawQuery string
	RequestHeaders     http.Header
	RequestBodyHash    string
	ResponseStatusCode int
	ResponseHeaders    http.Header
	ResponseBody       string
}

type Repository interface {
	// GetOrInsertKey returns the key if it exists, or inserts it if it doesn't.
	// If the key is inserted, the returned bool is true.
	GetOrInsert(*IdempotencyKey) (*IdempotencyKey, bool, error)
	// SetRecoveryPoint sets the recovery point for the key.
	SetRecoveryPoint(key string, recoveryPoint string) error
	// SetResponse sets the response fields for the key.
	SetResponse(key string, statusCode int, headers http.Header, body string) error
}

type Config struct {
	Locker               Locker
	Repository           Repository
	IdempotencyKeyHeader string
}

func New(config Config) func(http.Handler) http.Handler {
	if config.Locker == nil {
		panic("idempotency: Locker is required")
	}
	if config.Repository == nil {
		panic("idempotency: Repository is required")
	}
	if config.IdempotencyKeyHeader == "" {
		config.IdempotencyKeyHeader = DefaultIdempotencyKeyHeader
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get a hash of the request body to either compare with a stored request
			// or store it for future requests.
			hash, err := hashBody(r)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			key := &IdempotencyKey{
				Key:                r.Header.Get(config.IdempotencyKeyHeader),
				RecoveryPoint:      RecoveryPointStart,
				RequestMethod:      r.Method,
				RequestURLPath:     r.URL.Path,
				RequestURLRawQuery: r.URL.RawQuery,
				RequestHeaders:     r.Header,
				RequestBodyHash:    hash,
			}
			key, inserted, err := config.Repository.GetOrInsert(key)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			// Compare the incoming request with the stored request.
			if !inserted && !compareRequests(r, hash, key) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			// If the request has already been processed, return the stored response.
			if key.ResponseStatusCode != 0 {
				for k, v := range key.ResponseHeaders {
					w.Header().Add(k, strings.Join(v, ","))
				}
				w.WriteHeader(key.ResponseStatusCode)
				w.Write([]byte(key.ResponseBody))
				return
			}

			// Lock the key to prevent other requests from processing it.
			// TODO: lock timeout.
			acquired := config.Locker.Lock(key.Key)
			// If the lock was not acquired, return a 409.
			if !acquired {
				w.WriteHeader(http.StatusConflict)
				return
			}
			defer config.Locker.Unlock(key.Key)

			// Process the request and store the response.
			rec := httptest.NewRecorder()
			next.ServeHTTP(rec, r)

			result := rec.Result()
			resbody := rec.Body.Bytes()

			for k, v := range result.Header {
				w.Header().Add(k, strings.Join(v, ","))
			}

			w.WriteHeader(result.StatusCode)
			w.Write(resbody)

			err = config.Repository.SetResponse(key.Key, result.StatusCode,
				result.Header, string(resbody))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		})
	}
}

// Returns a hash of the request body.
func hashBody(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	// Restore the body so it can be read again.
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	h := md5.New()
	if _, err := h.Write(body); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Compares the incoming request with the stored request. Returns true if they
// match, false otherwise.
func compareRequests(r *http.Request,
	hashedBody string, stored *IdempotencyKey) bool {
	if r.Method != stored.RequestMethod {
		return false
	}

	if r.URL.Path != stored.RequestURLPath {
		return false
	}

	if r.URL.RawQuery != stored.RequestURLRawQuery {
		return false
	}

	if hashedBody != stored.RequestBodyHash {
		return false
	}

	if len(r.Header) != len(stored.RequestHeaders) {
		return false
	}

	requestHeaders := r.Header.Clone()

	for header, v := range stored.RequestHeaders {
		if requestHeaders.Get(header) != v[0] {
			return false
		}
		requestHeaders.Del(header)
	}

	return len(requestHeaders) == 0
}

type ctxKey struct{}

func withContext(r *http.Request, key *IdempotencyKey) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), ctxKey{}, key))
}

func fromContext(r *http.Request) *IdempotencyKey {
	return r.Context().Value(ctxKey{}).(*IdempotencyKey)
}

var rep Repository

func RecoveryPoint(name string, r *http.Request, fn func() string) {
	key := fromContext(r)
	if key == nil {
		panic("idempotency: RecoveryPoint must be called after idempotency middleware")
	}

	if key.RecoveryPoint != name {
		return
	}

	// TODO: prevent cyclic recovery points.

	next := fn()
	if err := rep.SetRecoveryPoint(key.Key, next); err != nil {
		panic(err)
	}

	key.RecoveryPoint = next
	r = withContext(r, key)
}
