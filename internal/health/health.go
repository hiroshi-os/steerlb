package health

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/picker"
)

type EndpointHealthState struct {
	Endpoint             *picker.Endpoint
	consecutiveSuccesses int
	consecutiveFailures  int
}

type Checker struct {
	clusterName string
	cfg         *config.HealthCheckConfig
	endpoints   []*picker.Endpoint
	metrics     *metrics.Metrics
	states      map[*picker.Endpoint]*EndpointHealthState
	client      *http.Client
	panicMode   atomic.Bool

	mu     sync.RWMutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
	seed   int64
}

func NewChecker(clusterName string, cfg *config.HealthCheckConfig, endpoints []*picker.Endpoint, m *metrics.Metrics, seed int64) *Checker {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	c := &Checker{
		clusterName: clusterName,
		cfg:         cfg,
		endpoints:   endpoints,
		metrics:     m,
		states:      make(map[*picker.Endpoint]*EndpointHealthState),
		seed:        seed,
		client: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
	}

	for _, ep := range endpoints {
		c.states[ep] = &EndpointHealthState{
			Endpoint: ep,
		}
		if m != nil {
			val := 0.0
			if ep.Healthy.Load() {
				val = 1.0
			}
			m.HealthStatus.WithLabelValues(ep.Addr).Set(val)
		}
	}
	c.recomputePanicMode()
	return c
}

func (c *Checker) Start(ctx context.Context) {
	c.mu.Lock()
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	rng := rand.New(rand.NewSource(c.seed))

	for i, ep := range c.endpoints {
		c.wg.Add(1)
		epSeed := rng.Int63() + int64(i)
		go c.checkLoop(ctx, ep, epSeed)
	}
}

func (c *Checker) Stop() {
	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
}

func (c *Checker) InPanicMode() bool {
	return c.panicMode.Load()
}

func (c *Checker) checkLoop(ctx context.Context, ep *picker.Endpoint, seed int64) {
	defer c.wg.Done()

	r := rand.New(rand.NewSource(seed))

	// Initial check immediately
	c.checkOnce(ctx, ep)

	for {
		interval := c.cfg.Interval
		if c.cfg.Jitter > 0 {
			jitterDelta := time.Duration(r.Int63n(int64(2*c.cfg.Jitter))) - c.cfg.Jitter
			interval += jitterDelta
			if interval <= 0 {
				interval = 50 * time.Millisecond
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			c.checkOnce(ctx, ep)
		}
	}
}

func (c *Checker) checkOnce(ctx context.Context, ep *picker.Endpoint) {
	url := fmt.Sprintf("http://%s%s", ep.Addr, c.cfg.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.recordResult(ep, false)
		return
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.recordResult(ep, false)
		return
	}
	_ = resp.Body.Close()

	success := resp.StatusCode >= 200 && resp.StatusCode <= 299
	c.recordResult(ep, success)
}

func (c *Checker) recordResult(ep *picker.Endpoint, success bool) {
	c.mu.Lock()
	state := c.states[ep]
	if state == nil {
		c.mu.Unlock()
		return
	}

	if success {
		state.consecutiveSuccesses++
		state.consecutiveFailures = 0
		if !ep.Healthy.Load() && state.consecutiveSuccesses >= c.cfg.HealthyThreshold {
			ep.Healthy.Store(true)
			if c.metrics != nil {
				c.metrics.HealthStatus.WithLabelValues(ep.Addr).Set(1.0)
			}
		}
	} else {
		state.consecutiveFailures++
		state.consecutiveSuccesses = 0
		if ep.Healthy.Load() && state.consecutiveFailures >= c.cfg.UnhealthyThreshold {
			ep.Healthy.Store(false)
			if c.metrics != nil {
				c.metrics.HealthStatus.WithLabelValues(ep.Addr).Set(0.0)
			}
		}
	}

	c.recomputePanicMode()
	c.mu.Unlock()
}

func (c *Checker) recomputePanicMode() {
	healthyCount := 0
	for _, ep := range c.endpoints {
		if ep.Healthy.Load() {
			healthyCount++
		}
	}

	isPanic := (healthyCount == 0 && len(c.endpoints) > 0)
	c.panicMode.Store(isPanic)

	if c.metrics != nil {
		val := 0.0
		if isPanic {
			val = 1.0
		}
		c.metrics.PanicMode.WithLabelValues(c.clusterName).Set(val)
	}
}
