package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ChaosConfig struct {
	mu             sync.RWMutex
	ErrRate        float64       `json:"err_rate"`
	SlowRate       float64       `json:"slow_rate"`
	SlowExtra      time.Duration `json:"slow_extra"`
	ResetRate      float64       `json:"reset_rate"`
	BlackholeRate  float64       `json:"blackhole_rate"`
	HealthzFail    bool          `json:"healthz_fail"`
	LatencyLogMu   float64       `json:"latency_log_mu"`
	LatencyLogSig  float64       `json:"latency_log_sig"`
	LatencyEnabled bool          `json:"latency_enabled"`
}

var (
	cfg       ChaosConfig
	backendID string
	rngMu     sync.Mutex
	rng       *rand.Rand
)

func parseLogNormal(dist string) (float64, float64, bool) {
	if dist == "" {
		return 0, 0, false
	}
	parts := strings.Split(dist, ":")
	if len(parts) != 2 || parts[0] != "lognormal" {
		return 0, 0, false
	}
	params := strings.Split(parts[1], ",")
	if len(params) != 2 {
		return 0, 0, false
	}
	mu, err1 := strconv.ParseFloat(params[0], 64)
	sig, err2 := strconv.ParseFloat(params[1], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return mu, sig, true
}

func main() {
	addr := flag.String("addr", ":9001", "Listen address")
	idFlag := flag.String("id", "", "Backend ID (defaults to addr)")
	latencyDist := flag.String("latency-dist", "", "Latency distribution (e.g. lognormal:1,0.3 in ms)")
	errRate := flag.Float64("err-rate", 0, "Rate of 503 errors (0.0 to 1.0)")
	slowRate := flag.Float64("slow-rate", 0, "Rate of slow responses (0.0 to 1.0)")
	slowExtra := flag.Duration("slow-extra", 200*time.Millisecond, "Extra latency for slow responses")
	resetRate := flag.Float64("reset-rate", 0, "Rate of hijacked connection resets (0.0 to 1.0)")
	blackholeRate := flag.Float64("blackhole-rate", 0, "Rate of blackholed requests (0.0 to 1.0)")
	healthzFail := flag.Bool("healthz-fail", false, "Toggle /healthz returning failure")
	seed := flag.Int64("seed", 0, "Random seed (defaults to current time)")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rng = rand.New(rand.NewSource(*seed))

	backendID = *idFlag
	if backendID == "" {
		backendID = *addr
	}

	mu, sig, hasLatency := parseLogNormal(*latencyDist)
	cfg = ChaosConfig{
		ErrRate:        *errRate,
		SlowRate:       *slowRate,
		SlowExtra:      *slowExtra,
		ResetRate:      *resetRate,
		BlackholeRate:  *blackholeRate,
		HealthzFail:    *healthzFail,
		LatencyLogMu:   mu,
		LatencyLogSig:  sig,
		LatencyEnabled: hasLatency,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/chaos", handleChaosAdmin)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/", handleMain)

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	slog.Info("chaosbackend starting", "addr", *addr, "id", backendID, "seed", *seed)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed", "err", err)
	}
}

func handleChaosAdmin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var update struct {
		ErrRate       *float64 `json:"err_rate"`
		SlowRate      *float64 `json:"slow_rate"`
		SlowExtra     *string  `json:"slow_extra"`
		ResetRate     *float64 `json:"reset_rate"`
		BlackholeRate *float64 `json:"blackhole_rate"`
		HealthzFail   *bool    `json:"healthz_fail"`
		LatencyDist   *string  `json:"latency_dist"`
	}

	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	cfg.mu.Lock()
	if update.ErrRate != nil {
		cfg.ErrRate = *update.ErrRate
	}
	if update.SlowRate != nil {
		cfg.SlowRate = *update.SlowRate
	}
	if update.SlowExtra != nil {
		if d, err := time.ParseDuration(*update.SlowExtra); err == nil {
			cfg.SlowExtra = d
		}
	}
	if update.ResetRate != nil {
		cfg.ResetRate = *update.ResetRate
	}
	if update.BlackholeRate != nil {
		cfg.BlackholeRate = *update.BlackholeRate
	}
	if update.HealthzFail != nil {
		cfg.HealthzFail = *update.HealthzFail
	}
	if update.LatencyDist != nil {
		mu, sig, has := parseLogNormal(*update.LatencyDist)
		cfg.LatencyLogMu = mu
		cfg.LatencyLogSig = sig
		cfg.LatencyEnabled = has
	}
	cfg.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	cfg.mu.RLock()
	fail := cfg.HealthzFail
	cfg.mu.RUnlock()

	w.Header().Set("X-Backend-Id", backendID)
	if fail {
		http.Error(w, "unhealthy", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func handleMain(w http.ResponseWriter, r *http.Request) {
	cfg.mu.RLock()
	errRate := cfg.ErrRate
	slowRate := cfg.SlowRate
	slowExtra := cfg.SlowExtra
	resetRate := cfg.ResetRate
	blackholeRate := cfg.BlackholeRate
	hasLat := cfg.LatencyEnabled
	latMu := cfg.LatencyLogMu
	latSig := cfg.LatencyLogSig
	cfg.mu.RUnlock()

	rngMu.Lock()
	rollErr := rng.Float64()
	rollSlow := rng.Float64()
	rollReset := rng.Float64()
	rollBlackhole := rng.Float64()
	z := rng.NormFloat64()
	rngMu.Unlock()

	// 1. Blackhole fault: accept and never respond
	if rollBlackhole < blackholeRate {
		select {
		case <-r.Context().Done():
			return
		}
	}

	// 2. Base latency distribution (lognormal in ms)
	if hasLat {
		ms := math.Exp(latMu + latSig*z)
		if ms > 0 {
			time.Sleep(time.Duration(ms * float64(time.Millisecond)))
		}
	}

	// 3. Slow fault: add extra latency
	if rollSlow < slowRate && slowExtra > 0 {
		time.Sleep(slowExtra)
	}

	// 4. Connection reset fault: hijack and close mid-response
	if rollReset < resetRate {
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nparti"))
				_ = conn.Close()
				return
			}
		}
	}

	w.Header().Set("X-Backend-Id", backendID)

	// 5. Error fault: return 503
	if rollErr < errRate {
		http.Error(w, "chaos backend error", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","backend":%q}`+"\n", backendID)
}
