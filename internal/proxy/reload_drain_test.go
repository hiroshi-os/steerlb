package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
)

func TestHotReload_ZeroErrorsUnderLoad(t *testing.T) {
	// Create 3 backends
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Id", "b1")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-b1"))
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Id", "b2")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-b2"))
	}))
	defer ts2.Close()

	ts3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Id", "b3")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok-b3"))
	}))
	defer ts3.Close()

	addr1 := strings.TrimPrefix(ts1.URL, "http://")
	addr2 := strings.TrimPrefix(ts2.URL, "http://")
	addr3 := strings.TrimPrefix(ts3.URL, "http://")

	cfgA := &config.Config{
		Listen: ":0", Admin: ":0",
		Clusters: []config.ClusterConfig{
			{Name: "cluster1", Endpoints: []string{addr1, addr2}, Picker: "round_robin"},
		},
		Routes: []config.RouteConfig{{PathPrefix: "/", Cluster: "cluster1"}},
	}

	cfgB := &config.Config{
		Listen: ":0", Admin: ":0",
		Clusters: []config.ClusterConfig{
			{Name: "cluster1", Endpoints: []string{addr2, addr3}, Picker: "round_robin"},
		},
		Routes: []config.RouteConfig{{PathPrefix: "/", Cluster: "cluster1"}},
	}

	m := metrics.NewMetrics()
	rtA, err := NewRuntime(cfgA, m, nil, 1)
	if err != nil {
		t.Fatalf("runtime creation error: %v", err)
	}

	p := NewProxy(rtA, m, nil, 1)
	proxyServer := httptest.NewServer(p)
	defer proxyServer.Close()

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	var totalRequests atomic.Int64
	var errorCount atomic.Int64
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup

	// Run concurrent workers sending requests
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopLoad:
					return
				default:
					resp, err := client.Get(proxyServer.URL + "/test")
					totalRequests.Add(1)
					if err != nil {
						errorCount.Add(1)
					} else {
						if resp.StatusCode != http.StatusOK {
							errorCount.Add(1)
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Send 20 reloads alternating between cfgA and cfgB
	for r := 0; r < 20; r++ {
		time.Sleep(10 * time.Millisecond)
		targetCfg := cfgB
		if r%2 == 0 {
			targetCfg = cfgA
		}

		newRt, err := NewRuntime(targetCfg, m, nil, int64(r+100))
		if err != nil {
			t.Fatalf("failed to create new runtime during reload %d: %v", r, err)
		}
		oldRt := p.SwapRuntime(newRt)
		if oldRt != nil {
			go func(old *Runtime) {
				time.Sleep(50 * time.Millisecond)
				old.Stop()
			}(oldRt)
		}
	}

	// Test rejecting invalid config while old keeps serving
	badYaml := `
listen: ":8080"
admin: ":9901"
clusters: []
routes: []
`
	_, badErr := config.ParseAndValidate([]byte(badYaml))
	if badErr == nil {
		t.Fatalf("expected invalid config to be rejected")
	}

	// Stop load workers
	close(stopLoad)
	wg.Wait()

	if totalRequests.Load() < 100 {
		t.Fatalf("insufficient requests run: %d", totalRequests.Load())
	}
	if errorCount.Load() != 0 {
		t.Fatalf("expected 0 client-visible errors during reloads, got %d out of %d requests", errorCount.Load(), totalRequests.Load())
	}
}

func TestGracefulDrain(t *testing.T) {
	hold := make(chan struct{})
	var backendRequests atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendRequests.Add(1)
		<-hold
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
	}))
	defer ts.Close()

	addr := strings.TrimPrefix(ts.URL, "http://")
	cfg := &config.Config{
		Listen: ":0", Admin: ":0",
		Clusters: []config.ClusterConfig{
			{Name: "c", Endpoints: []string{addr}, Picker: "round_robin"},
		},
		Routes: []config.RouteConfig{{PathPrefix: "/", Cluster: "c"}},
	}

	rt, _ := NewRuntime(cfg, nil, nil, 1)
	defer rt.Stop()

	p := NewProxy(rt, nil, nil, 1)

	var inFlightWg sync.WaitGroup
	var completedRequests atomic.Int64

	// Launch 5 in-flight requests that are currently held
	for i := 0; i < 5; i++ {
		inFlightWg.Add(1)
		go func() {
			defer inFlightWg.Done()
			req := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				completedRequests.Add(1)
			}
		}()
	}

	// Wait until requests reach backend
	for backendRequests.Load() < 5 {
		time.Sleep(5 * time.Millisecond)
	}

	// Trigger Drain in background
	drainDone := make(chan bool)
	go func() {
		drainDone <- p.Drain(5 * time.Second)
	}()

	// Wait until drain mode is active
	for !p.draining.Load() {
		time.Sleep(1 * time.Millisecond)
	}

	// While draining, new requests should be rejected with 503
	reqNew := httptest.NewRequest(http.MethodGet, "http://localhost/test", nil)
	recNew := httptest.NewRecorder()
	p.ServeHTTP(recNew, reqNew)
	if recNew.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected new request during drain to get 503, got %d", recNew.Code)
	}

	// Release in-flight requests
	close(hold)
	inFlightWg.Wait()

	drainedSuccessfully := <-drainDone
	if !drainedSuccessfully {
		t.Fatalf("expected drain to succeed cleanly")
	}
	if completedRequests.Load() != 5 {
		t.Fatalf("expected all 5 in-flight requests to complete, got %d", completedRequests.Load())
	}
}
