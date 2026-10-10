package outlier

import (
	"context"
	"sync"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/picker"
)

type EndpointState struct {
	Endpoint                  *picker.Endpoint
	Consecutive5xx            int
	ConsecutiveGatewayFailure int
	Ejected                   bool
	EjectionCount             int
	EjectedUntil              time.Time
}

type Detector struct {
	cfg       *config.OutlierConfig
	endpoints []*picker.Endpoint
	metrics   *metrics.Metrics
	clock     picker.Clock

	mu     sync.Mutex
	states map[*picker.Endpoint]*EndpointState
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewDetector(cfg *config.OutlierConfig, endpoints []*picker.Endpoint, m *metrics.Metrics, clk picker.Clock) *Detector {
	if clk == nil {
		clk = picker.RealClock{}
	}
	d := &Detector{
		cfg:       cfg,
		endpoints: endpoints,
		metrics:   m,
		clock:     clk,
		states:    make(map[*picker.Endpoint]*EndpointState),
	}

	for _, ep := range endpoints {
		d.states[ep] = &EndpointState{
			Endpoint: ep,
		}
		ep.Ejected.Store(false)
		if m != nil {
			m.OutlierEjected.WithLabelValues(ep.Addr).Set(0.0)
		}
	}

	return d
}

func (d *Detector) Start(ctx context.Context) {
	if d.cfg == nil || d.cfg.Interval <= 0 {
		return
	}
	d.mu.Lock()
	ctx, d.cancel = context.WithCancel(ctx)
	d.mu.Unlock()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(d.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.Sweep()
			}
		}
	}()
}

func (d *Detector) Stop() {
	d.mu.Lock()
	if d.cancel != nil {
		d.cancel()
	}
	d.mu.Unlock()
	d.wg.Wait()
}

func (d *Detector) Observe(ep *picker.Endpoint, outcome picker.Outcome) {
	if d.cfg == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	state, ok := d.states[ep]
	if !ok || state.Ejected {
		return
	}

	now := d.clock.Now()

	switch outcome {
	case picker.OutcomeSuccess:
		state.Consecutive5xx = 0
		state.ConsecutiveGatewayFailure = 0
		return

	case picker.OutcomeFail5xx:
		state.Consecutive5xx++
		state.ConsecutiveGatewayFailure = 0
		if d.cfg.Consecutive5xx > 0 && state.Consecutive5xx >= d.cfg.Consecutive5xx {
			d.attemptEjection(state, now)
		}

	case picker.OutcomeGatewayFail, picker.OutcomeTimeout:
		state.ConsecutiveGatewayFailure++
		state.Consecutive5xx = 0
		if d.cfg.ConsecutiveGatewayFailure > 0 && state.ConsecutiveGatewayFailure >= d.cfg.ConsecutiveGatewayFailure {
			d.attemptEjection(state, now)
		}
	}
}

func (d *Detector) attemptEjection(state *EndpointState, now time.Time) {
	n := len(d.endpoints)
	maxEjected := (n * d.cfg.MaxEjectionPercent) / 100

	// Invariant: if floor(max_ejection_percent * N / 100) == 0, nothing is ejected
	if maxEjected <= 0 {
		if d.metrics != nil {
			d.metrics.OutlierSkippedTotal.Inc()
		}
		state.Consecutive5xx = 0
		state.ConsecutiveGatewayFailure = 0
		return
	}

	// Count currently ejected endpoints
	currentEjected := 0
	for _, s := range d.states {
		if s.Ejected {
			currentEjected++
		}
	}

	if currentEjected >= maxEjected {
		// Cap reached: do not eject, count skipped
		if d.metrics != nil {
			d.metrics.OutlierSkippedTotal.Inc()
		}
		state.Consecutive5xx = 0
		state.ConsecutiveGatewayFailure = 0
		return
	}

	// Eject endpoint
	state.Ejected = true
	state.EjectionCount++
	state.Consecutive5xx = 0
	state.ConsecutiveGatewayFailure = 0

	ejectionDuration := d.cfg.BaseEjectionTime * time.Duration(state.EjectionCount)
	if ejectionDuration > d.cfg.MaxEjectionTime {
		ejectionDuration = d.cfg.MaxEjectionTime
	}

	state.EjectedUntil = now.Add(ejectionDuration)

	state.Endpoint.Ejected.Store(true)
	state.Endpoint.EjectionCount.Store(int64(state.EjectionCount))
	state.Endpoint.EjectUntilNano.Store(state.EjectedUntil.UnixNano())

	if d.metrics != nil {
		d.metrics.OutlierEjectionsTotal.Inc()
		d.metrics.OutlierEjected.WithLabelValues(state.Endpoint.Addr).Set(1.0)
	}
}

func (d *Detector) Sweep() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.clock.Now()
	for _, state := range d.states {
		if state.Ejected && (now.After(state.EjectedUntil) || now.Equal(state.EjectedUntil)) {
			state.Ejected = false
			state.Consecutive5xx = 0
			state.ConsecutiveGatewayFailure = 0
			state.Endpoint.Ejected.Store(false)
			if d.metrics != nil {
				d.metrics.OutlierEjected.WithLabelValues(state.Endpoint.Addr).Set(0.0)
			}
		}
	}
}

func (d *Detector) EjectedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	count := 0
	for _, s := range d.states {
		if s.Ejected {
			count++
		}
	}
	return count
}
