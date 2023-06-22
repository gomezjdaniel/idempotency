package idempotency

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ Locker = new(memoryLock)

type memoryLock struct {
	mu    sync.Mutex
	locks map[string]bool

	lockFn   func(string) bool
	unlockFn func(string)
}

func newMemoryLock() *memoryLock {
	return &memoryLock{locks: make(map[string]bool)}
}

func (l *memoryLock) Lock(key string) bool {
	if l.lockFn != nil {
		return l.lockFn(key)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.locks[key]; ok {
		return false
	}
	l.locks[key] = true
	return true
}

func (l *memoryLock) Unlock(key string) {
	if l.unlockFn != nil {
		l.unlockFn(key)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.locks, key)
}

var _ Repository = new(repository)

type repository struct {
	mu   sync.Mutex
	data map[string]*IdempotencyKey

	delayGet time.Duration
}

func newRepository() *repository {
	return &repository{data: make(map[string]*IdempotencyKey)}
}

func (r *repository) GetOrInsert(key *IdempotencyKey) (*IdempotencyKey, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.delayGet > 0 {
		time.Sleep(r.delayGet)
	}
	stored := r.data[key.Key]
	if stored == nil {
		r.data[key.Key] = key
		return key, true, nil
	}
	return stored, false, nil
}

func (r *repository) SetRecoveryPoint(key string, recoveryPoint string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key].RecoveryPoint = recoveryPoint
	return nil
}

func (r *repository) SetResponse(key string, statusCode int, headers http.Header, body string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key].ResponseStatusCode = statusCode
	r.data[key].ResponseHeaders = headers
	r.data[key].ResponseBody = body
	return nil
}

func TestBasicCase(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	middleware := New(Config{
		Locker:     newMemoryLock(),
		Repository: newRepository(),
	})

	counter := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter++
		w.Header().Set("counter", fmt.Sprintf("%d", counter))
		w.WriteHeader(210)
		w.Write([]byte(fmt.Sprintf("counter=%d", counter)))
	})

	ts := httptest.NewServer(middleware(handler))
	defer ts.Close()

	// First request
	req, err := http.NewRequest("POST", ts.URL, nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err := http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(210, res.StatusCode)
	a.Equal("1", res.Header.Get("counter"))

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	r.NoError(err)

	a.Equal(string(body), "counter=1")
	a.Equal(counter, 1)

	// Second request with same idempotency key
	req, err = http.NewRequest("POST", ts.URL, nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err = http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(210, res.StatusCode)

	body, err = ioutil.ReadAll(res.Body)
	res.Body.Close()
	r.NoError(err)

	a.Equal(string(body), "counter=1")
	a.Equal(counter, 1)
	a.Equal("1", res.Header.Get("counter"))

	// Third request with different idempotency key
	req, err = http.NewRequest("POST", ts.URL, nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "bar")

	res, err = http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(210, res.StatusCode)

	body, err = ioutil.ReadAll(res.Body)
	res.Body.Close()
	r.NoError(err)

	a.Equal(string(body), "counter=2")
	a.Equal(counter, 2)
	a.Equal("2", res.Header.Get("counter"))
}

func TestSameIdempotencyKeyDifferentQueryParams(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	middleware := New(Config{
		Locker:     newMemoryLock(),
		Repository: newRepository(),
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(middleware(handler))
	defer ts.Close()

	// First request
	req, err := http.NewRequest("POST", ts.URL+"?foo=bar", nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err := http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(http.StatusOK, res.StatusCode)

	// Second request with same idempotency key but different request params
	req, err = http.NewRequest("POST", ts.URL+"?foo=baz", nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err = http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(http.StatusBadRequest, res.StatusCode)
}

func TestSameIdempotencyKeyDifferentBody(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	middleware := New(Config{
		Locker:     newMemoryLock(),
		Repository: newRepository(),
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(middleware(handler))
	defer ts.Close()

	// First request
	req, err := http.NewRequest("POST", ts.URL, strings.NewReader("foo"))
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err := http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(http.StatusOK, res.StatusCode)

	// Second request with same idempotency key but different request params
	req, err = http.NewRequest("POST", ts.URL, strings.NewReader("bar"))
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err = http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(http.StatusBadRequest, res.StatusCode)
}

func TestConcurrentRequestsSameNewIdempotencyKey(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})

	middleware := New(Config{
		Locker:     newMemoryLock(),
		Repository: newRepository(),
	})

	ts := httptest.NewServer(middleware(handler))
	defer ts.Close()

	var (
		wg          sync.WaitGroup
		statusCodes []int
	)

	// First request
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		req, err := http.NewRequest("POST", ts.URL, nil)
		r.NoError(err)
		req.Header.Set("Idempotency-Key", "foo")

		res, err := http.DefaultClient.Do(req)
		r.NoError(err)
		statusCodes = append(statusCodes, res.StatusCode)
	}(&wg)

	// Second request with same idempotency key
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		req, err := http.NewRequest("POST", ts.URL, nil)
		r.NoError(err)
		req.Header.Set("Idempotency-Key", "foo")

		res, err := http.DefaultClient.Do(req)
		r.NoError(err)
		statusCodes = append(statusCodes, res.StatusCode)
	}(&wg)

	wg.Wait()

	sort.Ints(statusCodes)

	a.Equal([]int{http.StatusNoContent, http.StatusConflict}, statusCodes)
}

func TestConcurrentRequestsSameExistingIdempotencyKey(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	counter := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter++
		w.Header().Set("counter", fmt.Sprintf("%d", counter))
		w.WriteHeader(http.StatusOK)
	})

	repository := newRepository()
	// We will delay the get operation to simulate a slow repository.
	repository.delayGet = 500 * time.Millisecond

	middleware := New(Config{
		Locker:     newMemoryLock(),
		Repository: repository,
	})

	ts := httptest.NewServer(middleware(handler))
	defer ts.Close()

	// First request that will be stored in the repository.
	req, err := http.NewRequest("POST", ts.URL, nil)
	r.NoError(err)
	req.Header.Set("Idempotency-Key", "foo")

	res, err := http.DefaultClient.Do(req)
	r.NoError(err)
	a.Equal(http.StatusOK, res.StatusCode)
	a.Equal("1", res.Header.Get("counter"))

	// Now we will send two concurrent requests with the same idempotency key and
	// we expect them to be non-blocking and return the same response.

	var (
		wg          sync.WaitGroup
		statusCodes []int
	)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		req, err := http.NewRequest("POST", ts.URL, nil)
		r.NoError(err)
		req.Header.Set("Idempotency-Key", "foo")

		res, err := http.DefaultClient.Do(req)
		r.NoError(err)
		statusCodes = append(statusCodes, res.StatusCode)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		req, err := http.NewRequest("POST", ts.URL, nil)
		r.NoError(err)
		req.Header.Set("Idempotency-Key", "foo")

		res, err := http.DefaultClient.Do(req)
		r.NoError(err)
		statusCodes = append(statusCodes, res.StatusCode)
	}(&wg)

	wg.Wait()

	sort.Ints(statusCodes)

	a.Equal([]int{http.StatusOK, http.StatusOK}, statusCodes)
	a.Equal(1, counter)
}

func TestMultipleRecoveryPoints(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do some common logic.

		RecoveryPoint(RecoveryPointStart, r, func() string {
			// Do something.
			return "charge"
		})
		RecoveryPoint("charge", r, func() string {
			// Do something.
			return "send-email"
		})
		RecoveryPoint("send-email", r, func() string {
			// Do something.
			return ""
		})

		// Do some common logic.
	})

	_ = handler
}
