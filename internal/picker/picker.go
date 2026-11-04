package picker

import (
	"errors"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoEligibleEndpoints = errors.New("no eligible endpoints available")
)

type Outcome string

const (
	OutcomeSuccess     Outcome = "Success"
	OutcomeFail5xx     Outcome = "Fail5xx"
	OutcomeGatewayFail Outcome = "GatewayFail"
	OutcomeTimeout     Outcome = "Timeout"
)

type Clock interface {
	Now() time.Time
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

type Endpoint struct {
	Addr          string
	Inflight      atomic.Int64
	TotalRequests atomic.Uint64

	// Health and outlier detection
	Healthy                    atomic.Bool
	Ejected                    atomic.Bool
	EjectionCount              atomic.Int64
	EjectUntilNano             atomic.Int64 // unix nano
	Consecutive5xx             atomic.Int64
	ConsecutiveGatewayFailures atomic.Int64

	// Peak EWMA tracking
	clock        Clock
	tau          time.Duration
	ewmaMu       sync.Mutex
	ewmaDuration time.Duration
	lastObserved time.Time
}

func NewEndpoint(addr string, clock Clock, tau time.Duration) *Endpoint {
	if clock == nil {
		clock = RealClock{}
	}
	if tau <= 0 {
		tau = 10 * time.Second
	}
	ep := &Endpoint{
		Addr:  addr,
		clock: clock,
		tau:   tau,
	}
	ep.Healthy.Store(true)
	return ep
}

func (ep *Endpoint) GetEWMA() time.Duration {
	ep.ewmaMu.Lock()
	defer ep.ewmaMu.Unlock()
	return ep.currentDecayedEWMA(ep.clock.Now())
}

func (ep *Endpoint) currentDecayedEWMA(now time.Time) time.Duration {
	if ep.ewmaDuration <= 0 {
		return 1 * time.Millisecond // default baseline for unmeasured endpoints
	}
	if ep.lastObserved.IsZero() {
		return ep.ewmaDuration
	}
	dt := now.Sub(ep.lastObserved)
	if dt <= 0 {
		return ep.ewmaDuration
	}
	alpha := math.Exp(-float64(dt) / float64(ep.tau))
	decayed := time.Duration(float64(ep.ewmaDuration) * alpha)
	if decayed < 1*time.Microsecond {
		decayed = 1 * time.Microsecond
	}
	return decayed
}

func (ep *Endpoint) UpdateEWMA(sample time.Duration) {
	ep.ewmaMu.Lock()
	defer ep.ewmaMu.Unlock()

	now := ep.clock.Now()
	if ep.ewmaDuration <= 0 || ep.lastObserved.IsZero() {
		ep.ewmaDuration = sample
		ep.lastObserved = now
		return
	}

	decayed := ep.currentDecayedEWMA(now)
	if sample > decayed {
		// Sample above current value raises it immediately ("peak")
		ep.ewmaDuration = sample
	} else {
		dt := now.Sub(ep.lastObserved)
		alpha := math.Exp(-float64(dt) / float64(ep.tau))
		// Standard EWMA update on decayed value
		newVal := float64(sample)*(1.0-alpha) + float64(decayed)
		ep.ewmaDuration = time.Duration(newVal)
	}
	ep.lastObserved = now
}

func (ep *Endpoint) GetCost() float64 {
	ep.ewmaMu.Lock()
	ewma := ep.currentDecayedEWMA(ep.clock.Now())
	ep.ewmaMu.Unlock()

	inflight := ep.Inflight.Load()
	if inflight < 0 {
		inflight = 0
	}
	// cost = ewma_rtt * (inflight + 1)
	return float64(ewma.Nanoseconds()) * float64(inflight+1)
}

type Picker interface {
	Pick(r *http.Request, eligible []*Endpoint) (*Endpoint, error)
	Observe(ep *Endpoint, rtt time.Duration, outcome Outcome)
}

// RoundRobinPicker implements round-robin endpoint selection.
type RoundRobinPicker struct {
	counter atomic.Uint64
}

func NewRoundRobinPicker() *RoundRobinPicker {
	return &RoundRobinPicker{}
}

func (p *RoundRobinPicker) Pick(_ *http.Request, eligible []*Endpoint) (*Endpoint, error) {
	n := len(eligible)
	if n == 0 {
		return nil, ErrNoEligibleEndpoints
	}
	idx := p.counter.Add(1) - 1
	return eligible[idx%uint64(n)], nil
}

func (p *RoundRobinPicker) Observe(_ *Endpoint, _ time.Duration, _ Outcome) {
	// Round robin does not track observations
}

// LeastRequestP2CPicker implements power-of-two-choices least-request picking.
type LeastRequestP2CPicker struct {
	randMu sync.Mutex
	rng    *rand.Rand
}

func NewLeastRequestP2CPicker(seed int64) *LeastRequestP2CPicker {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &LeastRequestP2CPicker{
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (p *LeastRequestP2CPicker) Pick(_ *http.Request, eligible []*Endpoint) (*Endpoint, error) {
	n := len(eligible)
	if n == 0 {
		return nil, ErrNoEligibleEndpoints
	}
	if n == 1 {
		return eligible[0], nil
	}

	p.randMu.Lock()
	i := p.rng.Intn(n)
	j := p.rng.Intn(n - 1)
	p.randMu.Unlock()

	if j >= i {
		j++
	}

	ep1 := eligible[i]
	ep2 := eligible[j]

	if ep1.Inflight.Load() <= ep2.Inflight.Load() {
		return ep1, nil
	}
	return ep2, nil
}

func (p *LeastRequestP2CPicker) Observe(_ *Endpoint, _ time.Duration, _ Outcome) {
	// LeastRequest tracks inflight counts directly via Endpoint.Inflight
}

// P2CEWMAPicker implements power-of-two-choices peak-EWMA picking.
type P2CEWMAPicker struct {
	randMu sync.Mutex
	rng    *rand.Rand
}

func NewP2CEWMAPicker(seed int64) *P2CEWMAPicker {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &P2CEWMAPicker{
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (p *P2CEWMAPicker) Pick(_ *http.Request, eligible []*Endpoint) (*Endpoint, error) {
	n := len(eligible)
	if n == 0 {
		return nil, ErrNoEligibleEndpoints
	}
	if n == 1 {
		return eligible[0], nil
	}

	p.randMu.Lock()
	i := p.rng.Intn(n)
	j := p.rng.Intn(n - 1)
	p.randMu.Unlock()

	if j >= i {
		j++
	}

	ep1 := eligible[i]
	ep2 := eligible[j]

	cost1 := ep1.GetCost()
	cost2 := ep2.GetCost()

	if cost1 <= cost2 {
		return ep1, nil
	}
	return ep2, nil
}

func (p *P2CEWMAPicker) Observe(ep *Endpoint, rtt time.Duration, outcome Outcome) {
	if ep == nil {
		return
	}
	if outcome != OutcomeSuccess {
		// Finagle/Linkerd penalty: failures must never look faster than successes
		penalty := 1 * time.Second
		if rtt > penalty {
			penalty = rtt
		}
		ep.UpdateEWMA(penalty)
		return
	}
	ep.UpdateEWMA(rtt)
}
