package picker

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

type MockClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewMockClock(start time.Time) *MockClock {
	return &MockClock{now: start}
}

func (c *MockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *MockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestRoundRobinPicker(t *testing.T) {
	p := NewRoundRobinPicker()
	eps := []*Endpoint{
		NewEndpoint("127.0.0.1:9001", nil, 0),
		NewEndpoint("127.0.0.1:9002", nil, 0),
		NewEndpoint("127.0.0.1:9003", nil, 0),
	}

	for i := 0; i < 9; i++ {
		picked, err := p.Pick(nil, eps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := eps[i%3]
		if picked != expected {
			t.Errorf("iteration %d: got %s, want %s", i, picked.Addr, expected.Addr)
		}
	}

	// Empty list check
	_, err := p.Pick(nil, nil)
	if err != ErrNoEligibleEndpoints {
		t.Errorf("got %v, want ErrNoEligibleEndpoints", err)
	}
}

func TestLeastRequestP2C(t *testing.T) {
	p := NewLeastRequestP2CPicker(42)
	eps := []*Endpoint{
		NewEndpoint("127.0.0.1:9001", nil, 0),
		NewEndpoint("127.0.0.1:9002", nil, 0),
		NewEndpoint("127.0.0.1:9003", nil, 0),
	}

	eps[0].Inflight.Store(10)
	eps[1].Inflight.Store(2)
	eps[2].Inflight.Store(15)

	// In many picks, endpoint 1 should be picked frequently, and endpoint 0/2 only when paired together
	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		picked, err := p.Pick(nil, eps)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[picked.Addr]++
	}

	if counts["127.0.0.1:9002"] <= counts["127.0.0.1:9001"] || counts["127.0.0.1:9002"] <= counts["127.0.0.1:9003"] {
		t.Errorf("least inflight should be picked most frequently, got counts: %v", counts)
	}
}

func TestP2CEWMA_DecayAndPeak(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewMockClock(start)
	tau := 10 * time.Second

	ep := NewEndpoint("127.0.0.1:9001", clk, tau)
	picker := NewP2CEWMAPicker(100)

	// First observation sets the baseline
	picker.Observe(ep, 50*time.Millisecond, OutcomeSuccess)
	if ewma := ep.GetEWMA(); ewma != 50*time.Millisecond {
		t.Fatalf("initial ewma = %v, want 50ms", ewma)
	}

	// Higher sample should raise immediately ("peak")
	picker.Observe(ep, 100*time.Millisecond, OutcomeSuccess)
	if ewma := ep.GetEWMA(); ewma != 100*time.Millisecond {
		t.Fatalf("peak ewma = %v, want 100ms", ewma)
	}

	// Advance clock by tau (10s) -> should decay by ~e^-1 (~0.3678)
	clk.Advance(10 * time.Second)
	decayed := ep.GetEWMA()
	expectedDecayed := time.Duration(100.0 * math.Exp(-1.0) * float64(time.Millisecond))
	// Allow small tolerance
	diff := math.Abs(float64(decayed - expectedDecayed))
	if diff > float64(2*time.Millisecond) {
		t.Fatalf("decayed ewma after tau = %v, want ~%v", decayed, expectedDecayed)
	}

	// Lower sample at decayed state: should smooth
	clk.Advance(1 * time.Second)
	picker.Observe(ep, 20*time.Millisecond, OutcomeSuccess)
	if ep.GetEWMA() <= 0 {
		t.Fatalf("expected positive smoothed ewma, got %v", ep.GetEWMA())
	}
}

func TestP2CNeverPicksIneligible(t *testing.T) {
	p2c := NewP2CEWMAPicker(123)
	eligible := []*Endpoint{
		NewEndpoint("127.0.0.1:9001", nil, 0),
		NewEndpoint("127.0.0.1:9002", nil, 0),
	}
	ineligible := NewEndpoint("127.0.0.1:9003", nil, 0)

	for i := 0; i < 1000; i++ {
		picked, err := p2c.Pick(nil, eligible)
		if err != nil {
			t.Fatalf("unexpected pick error: %v", err)
		}
		if picked == ineligible || picked.Addr == ineligible.Addr {
			t.Fatalf("picked ineligible endpoint %s", picked.Addr)
		}
	}
}

func BenchmarkPickers(b *testing.B) {
	for _, numEndpoints := range []int{5, 100} {
		eps := make([]*Endpoint, numEndpoints)
		for i := 0; i < numEndpoints; i++ {
			eps[i] = NewEndpoint(fmt.Sprintf("10.0.0.%d:8080", i+1), nil, 10*time.Second)
			eps[i].UpdateEWMA(time.Duration(10+i) * time.Millisecond)
		}

		b.Run(fmt.Sprintf("RoundRobin/N=%d", numEndpoints), func(b *testing.B) {
			p := NewRoundRobinPicker()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = p.Pick(nil, eps)
				}
			})
		})

		b.Run(fmt.Sprintf("LeastRequestP2C/N=%d", numEndpoints), func(b *testing.B) {
			p := NewLeastRequestP2CPicker(1)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = p.Pick(nil, eps)
				}
			})
		})

		b.Run(fmt.Sprintf("P2C_EWMA/N=%d", numEndpoints), func(b *testing.B) {
			p := NewP2CEWMAPicker(1)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = p.Pick(nil, eps)
				}
			})
		})
	}
}
