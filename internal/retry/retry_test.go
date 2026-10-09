package retry

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"syscall"
	"testing"

	"pgregory.net/rapid"
)

func TestRetryEligibilityMatrix(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		headerKey string
		headerVal string
		bodySize  int64
		maxCap    int64
		retryable bool
	}{
		{"GET small body", "GET", "", "", 1024, 64 * 1024, true},
		{"GET 64KiB body", "GET", "", "", 64 * 1024, 64 * 1024, true},
		{"GET >64KiB body", "GET", "", "", 65 * 1024, 64 * 1024, false},
		{"POST small body without header", "POST", "", "", 1024, 64 * 1024, false},
		{"POST small body with X-Steer-Retryable true", "POST", "X-Steer-Retryable", "true", 1024, 64 * 1024, true},
		{"POST small body with X-Steer-Retryable false", "POST", "X-Steer-Retryable", "false", 1024, 64 * 1024, false},
		{"POST >64KiB with X-Steer-Retryable true", "POST", "X-Steer-Retryable", "true", 65 * 1024, 64 * 1024, false},
		{"PUT small body", "PUT", "", "", 1024, 64 * 1024, true},
		{"DELETE small body", "DELETE", "", "", 0, 64 * 1024, true},
		{"HEAD small body", "HEAD", "", "", 0, 64 * 1024, true},
		{"OPTIONS small body", "OPTIONS", "", "", 0, 64 * 1024, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://localhost/test", nil)
			if tt.headerKey != "" {
				req.Header.Set(tt.headerKey, tt.headerVal)
			}
			got := IsRequestRetryable(req, tt.bodySize, tt.maxCap)
			if got != tt.retryable {
				t.Errorf("got %v, want %v", got, tt.retryable)
			}
		})
	}
}

func TestMatchRetryCondition(t *testing.T) {
	retryOn := []string{"connect-failure", "reset", "502", "503", "504"}

	if !MatchRetryCondition(retryOn, 503, nil) {
		t.Errorf("expected 503 to match")
	}
	if !MatchRetryCondition(retryOn, 502, nil) {
		t.Errorf("expected 502 to match")
	}
	if !MatchRetryCondition(retryOn, 504, nil) {
		t.Errorf("expected 504 to match")
	}
	if MatchRetryCondition(retryOn, 500, nil) {
		t.Errorf("500 should not match when not in retryOn")
	}
	if !MatchRetryCondition(retryOn, 0, syscall.ECONNRESET) {
		t.Errorf("expected ECONNRESET to match reset")
	}
	if !MatchRetryCondition(retryOn, 0, errors.New("read: connection reset by peer")) {
		t.Errorf("expected connection reset error string to match reset")
	}
}

func TestBudgetMath(t *testing.T) {
	// budgetPercent: 20, minRetriesPerSec: 3
	b := NewBudget(20, 3, nil)

	// active = 0: allowance is max(3, 0) = 3
	if !b.TryAcquire() { // 1
		t.Fatalf("acquire 1 failed")
	}
	if !b.TryAcquire() { // 2
		t.Fatalf("acquire 2 failed")
	}
	if !b.TryAcquire() { // 3
		t.Fatalf("acquire 3 failed")
	}
	if b.TryAcquire() { // 4th should fail
		t.Fatalf("acquire 4 should have failed under min allowance 3")
	}

	b.Release() // in-flight now 2
	if !b.TryAcquire() {
		t.Fatalf("acquire should succeed after release")
	}

	// Now increase active requests to 100: allowance is max(3, 20% of 100 = 20) = 20
	for i := 0; i < 100; i++ {
		b.IncActive()
	}

	// Already have 3 in-flight, should be able to acquire up to 20
	for i := 3; i < 20; i++ {
		if !b.TryAcquire() {
			t.Fatalf("acquire %d failed under active=100 allowance=20", i+1)
		}
	}
	// 21st should fail
	if b.TryAcquire() {
		t.Fatalf("acquire 21 should fail under allowance 20")
	}
}

func TestBufferRequestBody(t *testing.T) {
	// Under cap
	data := []byte("hello world")
	req := httptest.NewRequest("POST", "/test", bytes.NewReader(data))
	buf, exceeded, err := BufferRequestBody(req, 1024)
	if err != nil || exceeded || string(buf) != "hello world" {
		t.Fatalf("expected buffered body, got exceeded=%v, err=%v, buf=%s", exceeded, err, string(buf))
	}

	// Verify request body can still be read
	readBack, err := io.ReadAll(req.Body)
	if err != nil || string(readBack) != "hello world" {
		t.Fatalf("expected readable body, got %s, err=%v", string(readBack), err)
	}

	// Exceeds cap
	bigData := make([]byte, 2048)
	reqBig := httptest.NewRequest("POST", "/test", bytes.NewReader(bigData))
	_, exceeded, err = BufferRequestBody(reqBig, 1024)
	if err != nil || !exceeded {
		t.Fatalf("expected exceeded=true for body > cap, got %v, err=%v", exceeded, err)
	}
}

func TestRetryBudget_PropertyBased(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		budgetPercent := rapid.IntRange(0, 100).Draw(rt, "budgetPercent")
		minRetriesPerSec := rapid.IntRange(1, 10).Draw(rt, "minRetriesPerSec")

		b := NewBudget(budgetPercent, minRetriesPerSec, nil)
		numSteps := rapid.IntRange(50, 200).Draw(rt, "numSteps")

		var activeCount int64
		var acquiredRetries int64

		for step := 0; step < numSteps; step++ {
			action := rapid.IntRange(0, 3).Draw(rt, "action")
			switch action {
			case 0: // Request arrives (active++)
				b.IncActive()
				activeCount++

			case 1: // Request completes (active--)
				if activeCount > 0 {
					b.DecActive()
					activeCount--
				}

			case 2: // Attempt to acquire retry
				active := b.ActiveCount()
				retriesBefore := b.RetriesInFlightCount()
				pctAllowance := (active * int64(budgetPercent)) / 100
				allowedMax := int64(minRetriesPerSec)
				if pctAllowance > allowedMax {
					allowedMax = pctAllowance
				}

				acquired := b.TryAcquire()
				if acquired {
					acquiredRetries++
					// Invariant: at the moment of issuance, retries before acquisition was strictly less than allowedMax
					if retriesBefore >= allowedMax {
						rt.Fatalf("step %d: retry was issued when retries %d >= allowedMax %d", step, retriesBefore, allowedMax)
					}
					if b.RetriesInFlightCount() > allowedMax {
						rt.Fatalf("step %d: retries in flight %d exceeded allowedMax %d immediately after issuance", step, b.RetriesInFlightCount(), allowedMax)
					}
				} else {
					// Invariant: acquisition rejected only when retriesBefore >= allowedMax
					if retriesBefore < allowedMax {
						rt.Fatalf("step %d: retry rejected when retriesBefore %d < allowedMax %d", step, retriesBefore, allowedMax)
					}
				}

			case 3: // Retry completes (release)
				if acquiredRetries > 0 {
					b.Release()
					acquiredRetries--
				}
			}
		}
	})
}
