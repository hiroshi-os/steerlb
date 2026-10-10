package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
)

func TestStripHopByHopHeaders(t *testing.T) {
	tests := []struct {
		name     string
		input    http.Header
		expected http.Header
	}{
		{
			name: "standard hop-by-hop headers removed",
			input: http.Header{
				"Connection":          []string{"close"},
				"Keep-Alive":          []string{"timeout=5"},
				"Proxy-Authenticate":  []string{"Basic"},
				"Proxy-Authorization": []string{"Bearer xyz"},
				"Te":                  []string{"trailers"},
				"Trailer":             []string{"Expires"},
				"Transfer-Encoding":   []string{"chunked"},
				"Upgrade":             []string{"websocket"},
				"X-Custom-Header":     []string{"keep-me"},
			},
			expected: http.Header{
				"X-Custom-Header": []string{"keep-me"},
			},
		},
		{
			name: "Connection header naming custom headers",
			input: http.Header{
				"Connection": []string{"X-Foo, X-Bar"},
				"X-Foo":      []string{"remove-me-foo"},
				"X-Bar":      []string{"remove-me-bar"},
				"X-Baz":      []string{"keep-me-baz"},
			},
			expected: http.Header{
				"X-Baz": []string{"keep-me-baz"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.input.Clone()
			StripHopByHopHeaders(h)

			for k := range tt.expected {
				if h.Get(k) != tt.expected.Get(k) {
					t.Errorf("header %s: got %q, want %q", k, h.Get(k), tt.expected.Get(k))
				}
			}
			for k := range h {
				if tt.expected.Get(k) == "" {
					t.Errorf("unexpected header remaining: %s: %q", k, h.Get(k))
				}
			}
		})
	}
}

func TestAppendXForwardedFor(t *testing.T) {
	h := make(http.Header)

	// First hop
	AppendXForwardedFor(h, "192.168.1.1:54321")
	if got := h.Get("X-Forwarded-For"); got != "192.168.1.1" {
		t.Fatalf("first hop: got %q, want 192.168.1.1", got)
	}

	// Second hop
	AppendXForwardedFor(h, "10.0.0.1:8080")
	if got := h.Get("X-Forwarded-For"); got != "192.168.1.1, 10.0.0.1" {
		t.Fatalf("second hop: got %q, want '192.168.1.1, 10.0.0.1'", got)
	}
}

func TestProxyIntegration_RetryAndBackends(t *testing.T) {
	var attempts atomic.Int64

	// Backend 1 returns 503
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("X-Backend-Id", "b1")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts1.Close()

	// Backend 2 returns 200 OK
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("X-Backend-Id", "b2")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success-from-b2"))
	}))
	defer ts2.Close()

	addr1 := strings.TrimPrefix(ts1.URL, "http://")
	addr2 := strings.TrimPrefix(ts2.URL, "http://")

	cfg := &config.Config{
		Listen: ":0",
		Admin:  ":0",
		Clusters: []config.ClusterConfig{
			{
				Name:      "test-cluster",
				Endpoints: []string{addr1, addr2},
				Picker:    "round_robin",
				Retry: &config.RetryConfig{
					MaxAttempts:      3,
					PerTryTimeout:    500 * time.Millisecond,
					RetryOn:          []string{"503"},
					BudgetPercent:    100,
					MinRetriesPerSec: 10,
					BackoffBase:      1 * time.Millisecond,
					BackoffMax:       5 * time.Millisecond,
				},
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/", Cluster: "test-cluster"},
		},
	}

	m := metrics.NewMetrics()
	rt, err := NewRuntime(cfg, m, nil, 42)
	if err != nil {
		t.Fatalf("NewRuntime error: %v", err)
	}
	defer rt.Stop()

	proxy := NewProxy(rt, m, nil, 42)

	// Make idempotent GET request
	req := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	resp := rec.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after retry, got %d (body: %s)", resp.StatusCode, string(body))
	}
	if string(body) != "success-from-b2" {
		t.Fatalf("expected 'success-from-b2', got %q", string(body))
	}
	if attempts.Load() < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", attempts.Load())
	}
}

func TestProxyIntegration_2MiBPostNeverRetried(t *testing.T) {
	var attempts atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	cfg := &config.Config{
		Listen: ":0",
		Admin:  ":0",
		Clusters: []config.ClusterConfig{
			{
				Name:      "test-cluster",
				Endpoints: []string{addr},
				Picker:    "round_robin",
				Retry: &config.RetryConfig{
					MaxAttempts:      3,
					PerTryTimeout:    500 * time.Millisecond,
					RetryOn:          []string{"503"},
					BudgetPercent:    100,
					MinRetriesPerSec: 10,
				},
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/", Cluster: "test-cluster"},
		},
	}

	rt, err := NewRuntime(cfg, nil, nil, 42)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	defer rt.Stop()

	proxy := NewProxy(rt, nil, nil, 42)

	// 2 MiB POST body
	twoMiB := make([]byte, 2*1024*1024)
	req := httptest.NewRequest(http.MethodPost, "http://localhost/upload", bytes.NewReader(twoMiB))
	req.Header.Set("X-Steer-Retryable", "true") // Even with retryable header, >64KiB must never retry!
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if attempts.Load() != 1 {
		t.Fatalf("2 MiB POST was retried %d times, expected exactly 1 attempt", attempts.Load())
	}
}

func TestProxyIntegration_ClientCancelPropagates(t *testing.T) {
	cancelled := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(cancelled)
	}))
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	cfg := &config.Config{
		Listen: ":0",
		Admin:  ":0",
		Clusters: []config.ClusterConfig{
			{
				Name:      "test-cluster",
				Endpoints: []string{addr},
				Picker:    "round_robin",
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/", Cluster: "test-cluster"},
		},
	}

	rt, _ := NewRuntime(cfg, nil, nil, 42)
	defer rt.Stop()

	proxy := NewProxy(rt, nil, nil, 42)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	proxy.ServeHTTP(rec, req)

	select {
	case <-cancelled:
		// Succeeded: backend saw context cancellation!
	case <-time.After(1 * time.Second):
		t.Fatalf("backend did not observe client cancellation")
	}
}

func TestProxyIntegration_CircuitBreaking(t *testing.T) {
	hold := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	cfg := &config.Config{
		Listen: ":0",
		Admin:  ":0",
		Clusters: []config.ClusterConfig{
			{
				Name:      "test-cluster",
				Endpoints: []string{addr},
				Picker:    "round_robin",
				Circuit: &config.CircuitConfig{
					MaxActiveRequests: 1,
					MaxPending:        0,
				},
			},
		},
		Routes: []config.RouteConfig{
			{PathPrefix: "/", Cluster: "test-cluster"},
		},
	}

	rt, _ := NewRuntime(cfg, nil, nil, 42)
	defer rt.Stop()

	proxy := NewProxy(rt, nil, nil, 42)

	// First request holds the single slot
	go func() {
		req1 := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
		rec1 := httptest.NewRecorder()
		proxy.ServeHTTP(rec1, req1)
	}()

	time.Sleep(30 * time.Millisecond)

	// Second request should trip circuit breaker
	req2 := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
	rec2 := httptest.NewRecorder()
	proxy.ServeHTTP(rec2, req2)

	close(hold)

	resp2 := rec2.Result()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 from tripped circuit breaker, got %d", resp2.StatusCode)
	}
	if resp2.Header.Get("X-Steer-Overloaded") != "1" {
		t.Fatalf("expected X-Steer-Overloaded: 1, got %q", resp2.Header.Get("X-Steer-Overloaded"))
	}
}
