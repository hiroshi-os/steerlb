package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/picker"
	"github.com/hiroshi-os/steerlb/internal/retry"
)

type Proxy struct {
	runtime        atomic.Pointer[Runtime]
	metrics        *metrics.Metrics
	clock          picker.Clock
	seed           int64
	draining       atomic.Bool
	activeRequests atomic.Int64
	drainCond      *sync.Cond
	drainMu        sync.Mutex
	rngMu          sync.Mutex
	rng            *rand.Rand
}

func NewProxy(initialRuntime *Runtime, m *metrics.Metrics, clk picker.Clock, seed int64) *Proxy {
	if clk == nil {
		clk = picker.RealClock{}
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	p := &Proxy{
		metrics: m,
		clock:   clk,
		seed:    seed,
		rng:     rand.New(rand.NewSource(seed)),
	}
	p.drainCond = sync.NewCond(&p.drainMu)
	p.runtime.Store(initialRuntime)
	return p
}

func (p *Proxy) SwapRuntime(newRuntime *Runtime) *Runtime {
	old := p.runtime.Swap(newRuntime)
	return old
}

func (p *Proxy) GetRuntime() *Runtime {
	return p.runtime.Load()
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.draining.Load() {
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return
	}

	p.activeRequests.Add(1)
	defer func() {
		p.activeRequests.Add(-1)
		p.drainMu.Lock()
		p.drainCond.Broadcast()
		p.drainMu.Unlock()
	}()

	startClient := p.clock.Now()
	defer func() {
		if p.metrics != nil {
			p.metrics.RequestDuration.Observe(time.Since(startClient).Seconds())
		}
	}()

	// 1. Request ID
	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = GenerateRequestID()
	}
	w.Header().Set("X-Request-Id", reqID)

	rt := p.runtime.Load()
	if rt == nil {
		http.Error(w, "proxy runtime not initialized", http.StatusServiceUnavailable)
		return
	}

	// 2. Route matching (longest prefix match)
	var matchedRoute *config.RouteConfig
	for i := range rt.Routes {
		route := &rt.Routes[i]
		if strings.HasPrefix(r.URL.Path, route.PathPrefix) {
			if matchedRoute == nil || len(route.PathPrefix) > len(matchedRoute.PathPrefix) {
				matchedRoute = route
			}
		}
	}

	if matchedRoute == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	cluster := rt.Clusters[matchedRoute.Cluster]
	if cluster == nil {
		http.Error(w, fmt.Sprintf("cluster %q not found", matchedRoute.Cluster), http.StatusBadGateway)
		return
	}

	// 3. Circuit breaking
	if cluster.Circuit != nil && cluster.Circuit.MaxActiveRequests > 0 {
		maxAllowed := int64(cluster.Circuit.MaxActiveRequests + cluster.Circuit.MaxPending)
		current := cluster.ActiveRequests.Add(1)
		defer cluster.ActiveRequests.Add(-1)

		if current > maxAllowed {
			w.Header().Set("X-Steer-Overloaded", "1")
			http.Error(w, "service overloaded", http.StatusServiceUnavailable)
			return
		}
	}

	// 4. Retry Budget active count
	if cluster.Budget != nil {
		cluster.Budget.IncActive()
		defer cluster.Budget.DecActive()
	}

	// 5. Body buffering
	bufferedBody, exceededCap, err := retry.BufferRequestBody(r, retry.DefaultMaxBodyCap)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	bodyLen := int64(len(bufferedBody))
	retryable := (!exceededCap) && retry.IsRequestRetryable(r, bodyLen, retry.DefaultMaxBodyCap)

	maxAttempts := 1
	var retryCfg *config.RetryConfig
	if cluster.RetryPolicy != nil {
		retryCfg = cluster.RetryPolicy.GetConfig()
		if retryable && retryCfg != nil && retryCfg.MaxAttempts > 1 {
			maxAttempts = retryCfg.MaxAttempts
		}
	}

	attempted := make(map[string]bool)
	var lastResp *http.Response
	var lastErr error
	var responseWritten bool

	p.rngMu.Lock()
	backoffRng := rand.New(rand.NewSource(p.rng.Int63()))
	p.rngMu.Unlock()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// If retrying, check budget and backoff
		if attempt > 1 {
			if cluster.Budget != nil {
				if !cluster.Budget.TryAcquire() {
					// Budget exhausted
					slog.Debug("retry budget exhausted", "req_id", reqID, "cluster", cluster.Name)
					break
				}
				defer cluster.Budget.Release()
			}

			if p.metrics != nil {
				p.metrics.RetriesTotal.Inc()
			}

			// Backoff between attempts
			if retryCfg != nil && retryCfg.BackoffBase > 0 {
				backoff := retry.CalculateBackoff(attempt-1, retryCfg.BackoffBase, retryCfg.BackoffMax, backoffRng)
				if err := retry.SleepWithContext(r.Context(), backoff); err != nil {
					// Client cancelled
					lastErr = err
					break
				}
			}
		}

		// Pick eligible endpoint
		eligible := cluster.EligibleEndpoints(attempted)
		if len(eligible) == 0 {
			lastErr = picker.ErrNoEligibleEndpoints
			break
		}

		picked, err := cluster.Picker.Pick(r, eligible)
		if err != nil {
			lastErr = err
			break
		}

		attempted[picked.Addr] = true

		// Per-try context and timeout
		tryCtx := r.Context()
		var cancel context.CancelFunc
		if retryCfg != nil && retryCfg.PerTryTimeout > 0 {
			tryCtx, cancel = context.WithTimeout(r.Context(), retryCfg.PerTryTimeout)
		}

		// Construct upstream request
		upstreamReq := r.Clone(tryCtx)
		upstreamReq.URL.Scheme = "http"
		upstreamReq.URL.Host = picked.Addr
		upstreamReq.Host = picked.Addr

		StripHopByHopHeaders(upstreamReq.Header)
		AppendXForwardedFor(upstreamReq.Header, r.RemoteAddr)
		if r.TLS != nil {
			upstreamReq.Header.Set("X-Forwarded-Proto", "https")
		} else {
			upstreamReq.Header.Set("X-Forwarded-Proto", "http")
		}
		upstreamReq.Header.Set("X-Request-Id", reqID)

		if bufferedBody != nil {
			upstreamReq.Body = io.NopCloser(bytes.NewReader(bufferedBody))
			upstreamReq.ContentLength = int64(len(bufferedBody))
		}

		// Execute upstream request
		picked.Inflight.Add(1)
		picked.TotalRequests.Add(1)
		if p.metrics != nil {
			p.metrics.InflightRequests.WithLabelValues(picked.Addr).Set(float64(picked.Inflight.Load()))
		}

		startUpstream := p.clock.Now()
		resp, roundTripErr := cluster.Transport.RoundTrip(upstreamReq)
		upstreamDuration := time.Since(startUpstream)

		picked.Inflight.Add(-1)
		if p.metrics != nil {
			p.metrics.InflightRequests.WithLabelValues(picked.Addr).Set(float64(picked.Inflight.Load()))
			p.metrics.UpstreamDuration.WithLabelValues(picked.Addr).Observe(upstreamDuration.Seconds())
		}

		if cancel != nil {
			cancel()
		}

		// Determine Outcome
		var outcome picker.Outcome
		if roundTripErr != nil {
			if errors.Is(tryCtx.Err(), context.DeadlineExceeded) {
				outcome = picker.OutcomeTimeout
			} else {
				outcome = picker.OutcomeGatewayFail
			}
		} else {
			if resp.StatusCode >= 500 {
				outcome = picker.OutcomeFail5xx
			} else {
				outcome = picker.OutcomeSuccess
			}
		}

		// Observe outcome and latency
		cluster.Picker.Observe(picked, upstreamDuration, outcome)
		if cluster.OutlierDetector != nil {
			cluster.OutlierDetector.Observe(picked, outcome)
		}

		if p.metrics != nil {
			codeStr := "0"
			if resp != nil {
				codeStr = fmt.Sprintf("%d", resp.StatusCode)
			}
			p.metrics.RequestsTotal.WithLabelValues(cluster.Name, picked.Addr, codeStr).Inc()
		}

		lastResp = resp
		lastErr = roundTripErr

		// Check if we should retry
		shouldRetry := false
		if retryCfg != nil && retryable {
			statusCode := 0
			if resp != nil {
				statusCode = resp.StatusCode
			}
			if retry.MatchRetryCondition(retryCfg.RetryOn, statusCode, roundTripErr) {
				shouldRetry = true
			}
		}

		if shouldRetry && attempt < maxAttempts {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			continue
		}

		// Success or not retryable: write response to client
		if roundTripErr != nil {
			if errors.Is(roundTripErr, context.DeadlineExceeded) || errors.Is(r.Context().Err(), context.DeadlineExceeded) {
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			} else {
				http.Error(w, "bad gateway", http.StatusBadGateway)
			}
			return
		}

		// Stream response body directly to client
		StripHopByHopHeaders(resp.Header)
		for k, vals := range resp.Header {
			for _, val := range vals {
				w.Header().Add(k, val)
			}
		}
		w.WriteHeader(resp.StatusCode)
		responseWritten = true

		if resp.Body != nil {
			_, _ = io.Copy(w, resp.Body)
			_ = resp.Body.Close()
		}
		return
	}

	// If loop terminated without writing response
	if !responseWritten {
		if lastResp != nil {
			StripHopByHopHeaders(lastResp.Header)
			for k, vals := range lastResp.Header {
				for _, val := range vals {
					w.Header().Add(k, val)
				}
			}
			w.WriteHeader(lastResp.StatusCode)
			if lastResp.Body != nil {
				_, _ = io.Copy(w, lastResp.Body)
				_ = lastResp.Body.Close()
			}
		} else {
			if errors.Is(lastErr, picker.ErrNoEligibleEndpoints) {
				http.Error(w, "no healthy upstream endpoints", http.StatusServiceUnavailable)
			} else if errors.Is(lastErr, context.DeadlineExceeded) {
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			} else {
				http.Error(w, "bad gateway", http.StatusBadGateway)
			}
		}
	}
}

// Drain waits for active in-flight requests to complete up to timeout.
func (p *Proxy) Drain(timeout time.Duration) bool {
	p.draining.Store(true)

	deadline := time.Now().Add(timeout)
	p.drainMu.Lock()
	defer p.drainMu.Unlock()

	for p.activeRequests.Load() > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false // timed out with requests still in flight
		}

		// Wait with a small polling timer or condition broadcast
		done := make(chan struct{})
		go func() {
			time.Sleep(remaining)
			p.drainMu.Lock()
			p.drainCond.Broadcast()
			p.drainMu.Unlock()
			close(done)
		}()

		p.drainCond.Wait()
		if time.Now().After(deadline) {
			break
		}
	}

	return p.activeRequests.Load() == 0
}
