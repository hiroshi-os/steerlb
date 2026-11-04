package retry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
)

const DefaultMaxBodyCap = 64 * 1024 // 64 KiB

type Budget struct {
	budgetPercent    int
	minRetriesPerSec int
	activeRequests   atomic.Int64
	retriesInFlight  atomic.Int64
	metrics          *metrics.Metrics
}

func NewBudget(budgetPercent, minRetriesPerSec int, m *metrics.Metrics) *Budget {
	return &Budget{
		budgetPercent:    budgetPercent,
		minRetriesPerSec: minRetriesPerSec,
		metrics:          m,
	}
}

func (b *Budget) IncActive() {
	b.activeRequests.Add(1)
}

func (b *Budget) DecActive() {
	b.activeRequests.Add(-1)
}

func (b *Budget) ActiveCount() int64 {
	return b.activeRequests.Load()
}

func (b *Budget) RetriesInFlightCount() int64 {
	return b.retriesInFlight.Load()
}

func (b *Budget) TryAcquire() bool {
	minAllowance := int64(b.minRetriesPerSec)
	for {
		active := b.activeRequests.Load()
		if active < 0 {
			active = 0
		}
		retries := b.retriesInFlight.Load()
		pctAllowance := (active * int64(b.budgetPercent)) / 100
		allowedMax := minAllowance
		if pctAllowance > allowedMax {
			allowedMax = pctAllowance
		}

		if retries >= allowedMax {
			if b.metrics != nil {
				b.metrics.RetryBudgetExhausted.Inc()
			}
			return false
		}

		if b.retriesInFlight.CompareAndSwap(retries, retries+1) {
			return true
		}
	}
}

func (b *Budget) Release() {
	b.retriesInFlight.Add(-1)
}

type Policy struct {
	cfg     *config.RetryConfig
	budget  *Budget
	metrics *metrics.Metrics
}

func NewPolicy(cfg *config.RetryConfig, budget *Budget, m *metrics.Metrics) *Policy {
	return &Policy{
		cfg:     cfg,
		budget:  budget,
		metrics: m,
	}
}

func (p *Policy) GetConfig() *config.RetryConfig {
	return p.cfg
}

func (p *Policy) GetBudget() *Budget {
	return p.budget
}

func IsIdempotentMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete, "TRACE":
		return true
	default:
		return false
	}
}

func IsRequestRetryable(r *http.Request, bodyLen int64, maxBodyCap int64) bool {
	if maxBodyCap <= 0 {
		maxBodyCap = DefaultMaxBodyCap
	}
	if bodyLen > maxBodyCap {
		return false
	}

	if IsIdempotentMethod(r.Method) {
		return true
	}

	// Non-idempotent methods can be retried only if X-Steer-Retryable is true
	if strings.EqualFold(r.Header.Get("X-Steer-Retryable"), "true") {
		return true
	}

	return false
}

func MatchRetryCondition(retryOn []string, statusCode int, err error) bool {
	if len(retryOn) == 0 {
		return false
	}

	for _, cond := range retryOn {
		cond = strings.TrimSpace(strings.ToLower(cond))
		if cond == "" {
			continue
		}

		// Status code check (e.g. "502", "503", "504")
		if code, convErr := strconv.Atoi(cond); convErr == nil {
			if statusCode == code {
				return true
			}
			continue
		}

		if err == nil {
			continue
		}

		// Connect failure check
		if cond == "connect-failure" {
			var netOpErr *net.OpError
			if errors.As(err, &netOpErr) && netOpErr.Op == "dial" {
				return true
			}
		}

		// Reset / unexpected EOF check
		if cond == "reset" {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
				return true
			}
			if strings.Contains(strings.ToLower(err.Error()), "connection reset") {
				return true
			}
		}
	}

	return false
}

func CalculateBackoff(attempt int, base, max time.Duration, rng *rand.Rand) time.Duration {
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	if max < base {
		max = base
	}

	multiplier := 1 << (attempt - 1)
	if multiplier > 1024 {
		multiplier = 1024
	}

	target := base * time.Duration(multiplier)
	if target > max {
		target = max
	}

	if rng != nil {
		jitter := time.Duration(rng.Int63n(int64(target) + 1))
		return jitter
	}
	return target
}

// BufferRequestBody buffers request body up to maxCap.
// If body exceeds maxCap, returns original reader and exceeds=true.
func BufferRequestBody(r *http.Request, maxCap int64) ([]byte, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false, nil
	}

	if maxCap <= 0 {
		maxCap = DefaultMaxBodyCap
	}

	// Fast path Content-Length check
	if r.ContentLength > maxCap {
		return nil, true, nil
	}

	// Read up to maxCap + 1 bytes to detect if it exceeds cap
	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	lr := io.LimitReader(r.Body, maxCap+1)
	n, err := buf.ReadFrom(lr)
	if err != nil {
		return nil, false, err
	}

	if n > maxCap {
		// Restore body by combining what was read with remaining body
		r.Body = io.NopCloser(io.MultiReader(buf, r.Body))
		return nil, true, nil
	}

	data := buf.Bytes()
	r.Body = io.NopCloser(bytes.NewReader(data))
	return data, false, nil
}

func SleepWithContext(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
