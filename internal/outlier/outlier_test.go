package outlier

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/picker"
	"pgregory.net/rapid"
)

type MockClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewMockClock(t time.Time) *MockClock {
	return &MockClock{now: t}
}

func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *MockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func TestOutlierDetector_Basic(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewMockClock(start)

	ep1 := picker.NewEndpoint("127.0.0.1:9001", clk, 0)
	ep2 := picker.NewEndpoint("127.0.0.1:9002", clk, 0)
	endpoints := []*picker.Endpoint{ep1, ep2}

	cfg := &config.OutlierConfig{
		Consecutive5xx:            3,
		ConsecutiveGatewayFailure: 2,
		BaseEjectionTime:          5 * time.Second,
		MaxEjectionTime:           30 * time.Second,
		MaxEjectionPercent:        50, // 50% of 2 is 1 endpoint max
		Interval:                  1 * time.Second,
	}

	det := NewDetector(cfg, endpoints, nil, clk)

	// 2 consecutive 5xx: not yet ejected
	det.Observe(ep1, picker.OutcomeFail5xx)
	det.Observe(ep1, picker.OutcomeFail5xx)
	if ep1.Ejected.Load() {
		t.Fatalf("ep1 should not be ejected after 2 failures")
	}

	// 3rd consecutive 5xx: ejected!
	det.Observe(ep1, picker.OutcomeFail5xx)
	if !ep1.Ejected.Load() {
		t.Fatalf("ep1 should be ejected after 3 consecutive 5xx")
	}

	// Now try to eject ep2: since max ejection percent is 50% (1 of 2), ep2 must NOT be ejected!
	det.Observe(ep2, picker.OutcomeFail5xx)
	det.Observe(ep2, picker.OutcomeFail5xx)
	det.Observe(ep2, picker.OutcomeFail5xx)
	if ep2.Ejected.Load() {
		t.Fatalf("ep2 should not be ejected due to max_ejection_percent cap")
	}

	// Advance clock by 4 seconds (less than 5s base ejection time)
	clk.Advance(4 * time.Second)
	det.Sweep()
	if !ep1.Ejected.Load() {
		t.Fatalf("ep1 should still be ejected after 4s")
	}

	// Advance clock by 2 more seconds (total 6s > 5s)
	clk.Advance(2 * time.Second)
	det.Sweep()
	if ep1.Ejected.Load() {
		t.Fatalf("ep1 should be unejected after base ejection time")
	}
}

func TestOutlierDetector_PropertyBased(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		numEndpoints := rapid.IntRange(2, 20).Draw(rt, "numEndpoints")
		maxEjectionPercent := rapid.IntRange(0, 100).Draw(rt, "maxEjectionPercent")
		consecutive5xx := rapid.IntRange(1, 5).Draw(rt, "consecutive5xx")
		consecutiveGw := rapid.IntRange(1, 3).Draw(rt, "consecutiveGw")
		baseSec := rapid.IntRange(1, 5).Draw(rt, "baseSec")
		maxSec := rapid.IntRange(baseSec, 30).Draw(rt, "maxSec")

		cfg := &config.OutlierConfig{
			Consecutive5xx:            consecutive5xx,
			ConsecutiveGatewayFailure: consecutiveGw,
			BaseEjectionTime:          time.Duration(baseSec) * time.Second,
			MaxEjectionTime:           time.Duration(maxSec) * time.Second,
			MaxEjectionPercent:        maxEjectionPercent,
			Interval:                  time.Second,
		}

		start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		clk := NewMockClock(start)

		endpoints := make([]*picker.Endpoint, numEndpoints)
		for i := 0; i < numEndpoints; i++ {
			endpoints[i] = picker.NewEndpoint(fmt.Sprintf("ep-%d:8080", i), clk, 0)
		}

		det := NewDetector(cfg, endpoints, nil, clk)
		p2c := picker.NewP2CEWMAPicker(42)

		numSteps := rapid.IntRange(20, 100).Draw(rt, "numSteps")
		floorCap := (numEndpoints * maxEjectionPercent) / 100

		for step := 0; step < numSteps; step++ {
			action := rapid.IntRange(0, 2).Draw(rt, "action")
			switch action {
			case 0: // Observe
				epIdx := rapid.IntRange(0, numEndpoints-1).Draw(rt, "epIdx")
				outcomeType := rapid.IntRange(0, 3).Draw(rt, "outcomeType")
				var oc picker.Outcome
				switch outcomeType {
				case 0:
					oc = picker.OutcomeSuccess
				case 1:
					oc = picker.OutcomeFail5xx
				case 2:
					oc = picker.OutcomeGatewayFail
				case 3:
					oc = picker.OutcomeTimeout
				}
				det.Observe(endpoints[epIdx], oc)

			case 1: // Advance clock
				secs := rapid.IntRange(1, 10).Draw(rt, "advanceSecs")
				clk.Advance(time.Duration(secs) * time.Second)

			case 2: // Sweep
				det.Sweep()
			}

			// Invariant (a): number of ejected endpoints <= floor(max_ejection_percent * N / 100)
			ejectedCount := det.EjectedCount()
			if floorCap == 0 {
				if ejectedCount != 0 {
					rt.Fatalf("step %d: floorCap is 0 but %d endpoints are ejected", step, ejectedCount)
				}
			} else {
				if ejectedCount > floorCap {
					rt.Fatalf("step %d: ejectedCount %d exceeds cap %d", step, ejectedCount, floorCap)
				}
			}

			// Invariant (b): an ejected endpoint is never returned by picker over eligible endpoints
			var eligible []*picker.Endpoint
			for _, ep := range endpoints {
				if !ep.Ejected.Load() {
					eligible = append(eligible, ep)
				}
			}
			if len(eligible) > 0 {
				picked, err := p2c.Pick(nil, eligible)
				if err != nil {
					rt.Fatalf("step %d: unexpected pick error: %v", step, err)
				}
				if picked.Ejected.Load() {
					rt.Fatalf("step %d: picker returned ejected endpoint %s", step, picked.Addr)
				}
			}

			// Invariant (c): ejection duration is non-decreasing in ejection count and never exceeds max_ejection_time
			det.mu.Lock()
			for _, s := range det.states {
				if s.EjectionCount > 0 {
					dur := cfg.BaseEjectionTime * time.Duration(s.EjectionCount)
					if dur > cfg.MaxEjectionTime {
						dur = cfg.MaxEjectionTime
					}
					if dur > cfg.MaxEjectionTime {
						rt.Fatalf("step %d: ejection duration %v exceeds max %v", step, dur, cfg.MaxEjectionTime)
					}
				}
			}
			det.mu.Unlock()
		}

		// Invariant (d): once no failures occur for max_ejection_time plus one sweep, every endpoint is eligible
		clk.Advance(cfg.MaxEjectionTime + time.Second)
		det.Sweep()
		for _, ep := range endpoints {
			if ep.Ejected.Load() {
				rt.Fatalf("endpoint %s still ejected after max_ejection_time + 1s sweep", ep.Addr)
			}
		}
	})
}
