package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/picker"
)

func TestHealthCheckTransitionsAndPanicMode(t *testing.T) {
	var shouldFail atomic.Bool

	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts2.Close()

	addr1 := strings.TrimPrefix(ts1.URL, "http://")
	addr2 := strings.TrimPrefix(ts2.URL, "http://")

	ep1 := picker.NewEndpoint(addr1, nil, 0)
	ep2 := picker.NewEndpoint(addr2, nil, 0)
	endpoints := []*picker.Endpoint{ep1, ep2}

	cfg := &config.HealthCheckConfig{
		Path:               "/healthz",
		Interval:           50 * time.Millisecond,
		Timeout:            50 * time.Millisecond,
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
		Jitter:             0,
	}

	m := metrics.NewMetrics()
	checker := NewChecker("test_cluster", cfg, endpoints, m, 42)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker.Start(ctx)
	defer checker.Stop()

	// Initial check: both should be healthy, not panic
	time.Sleep(100 * time.Millisecond)
	if !ep1.Healthy.Load() || !ep2.Healthy.Load() {
		t.Fatalf("expected both endpoints healthy initially")
	}
	if checker.InPanicMode() {
		t.Fatalf("expected not in panic mode")
	}

	// Trigger failures on backends
	shouldFail.Store(true)

	// After 1 failure, threshold (2) is not yet reached
	time.Sleep(40 * time.Millisecond)

	// After >2 intervals, both should be marked unhealthy and panic mode entered
	time.Sleep(200 * time.Millisecond)
	if ep1.Healthy.Load() || ep2.Healthy.Load() {
		t.Fatalf("expected endpoints unhealthy after failures, got ep1=%v, ep2=%v", ep1.Healthy.Load(), ep2.Healthy.Load())
	}
	if !checker.InPanicMode() {
		t.Fatalf("expected panic mode when all endpoints unhealthy")
	}

	// Now recover backends
	shouldFail.Store(false)
	time.Sleep(250 * time.Millisecond)

	if !ep1.Healthy.Load() || !ep2.Healthy.Load() {
		t.Fatalf("expected endpoints recovered to healthy, got ep1=%v, ep2=%v", ep1.Healthy.Load(), ep2.Healthy.Load())
	}
	if checker.InPanicMode() {
		t.Fatalf("expected panic mode cleared after recovery")
	}
}
