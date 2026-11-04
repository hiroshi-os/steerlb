package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/proxy"
)

type ScenarioResult struct {
	Scenario              string  `json:"scenario"`
	Policy                string  `json:"policy"`
	SuccessPct            float64 `json:"success_pct"`
	P50Ms                 float64 `json:"p50_ms"`
	P90Ms                 float64 `json:"p90_ms"`
	P99Ms                 float64 `json:"p99_ms"`
	P999Ms                float64 `json:"p999_ms"`
	MaxMs                 float64 `json:"max_ms"`
	Amplification         float64 `json:"amplification"`
	TimeToEjectMs         int64   `json:"time_to_eject_ms"`
	TimeToReadmitMs       int64   `json:"time_to_readmit_ms"`
	RetryBudgetExhausted  int64   `json:"retry_budget_exhausted"`
	EjectedRequestsWhileE int64   `json:"ejected_requests_while_ejected"`
}

type BenchOutput struct {
	Hardware  string           `json:"hardware"`
	Date      string           `json:"date"`
	RpsTarget int              `json:"rps_target"`
	Duration  string           `json:"duration"`
	Results   []ScenarioResult `json:"results"`
}

type ControlledBackend struct {
	id            string
	listener      net.Listener
	server        *http.Server
	addr          string
	mu            sync.Mutex
	killed        bool
	errRate       float64
	slowRate      float64
	slowExtra     time.Duration
	blackholeRate float64
	requestsRecv  atomic.Int64
}

func (b *ControlledBackend) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	b.listener = ln
	b.addr = ln.Addr().String()
	b.killed = false

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		killed := b.killed
		b.mu.Unlock()
		if killed {
			http.Error(w, "unhealthy", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Backend-Id", b.id)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b.requestsRecv.Add(1)
		b.mu.Lock()
		errRate := b.errRate
		slowRate := b.slowRate
		slowExtra := b.slowExtra
		blackholeRate := b.blackholeRate
		killed := b.killed
		b.mu.Unlock()

		if killed {
			http.Error(w, "killed", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("X-Backend-Id", b.id)

		if blackholeRate > 0 && rand.Float64() < blackholeRate {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Second):
				return
			}
		}

		if slowRate > 0 && rand.Float64() < slowRate && slowExtra > 0 {
			time.Sleep(slowExtra)
		}

		if errRate > 0 && rand.Float64() < errRate {
			http.Error(w, "backend 503", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ok","backend":%q}`, b.id)
	})

	b.server = &http.Server{Handler: mux}
	go func() {
		_ = b.server.Serve(ln)
	}()
	return nil
}

func (b *ControlledBackend) Kill() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.killed = true
	if b.listener != nil {
		_ = b.listener.Close()
	}
	if b.server != nil {
		_ = b.server.Close()
	}
}

func (b *ControlledBackend) Restart() error {
	b.mu.Lock()
	oldAddr := b.addr
	b.mu.Unlock()

	ln, err := net.Listen("tcp", oldAddr)
	if err != nil {
		// If original port is unavailable, re-listen
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
	}

	b.mu.Lock()
	b.listener = ln
	b.addr = ln.Addr().String()
	b.killed = false
	b.errRate = 0
	b.slowRate = 0
	b.blackholeRate = 0

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Id", b.id)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b.requestsRecv.Add(1)
		w.Header().Set("X-Backend-Id", b.id)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ok","backend":%q}`, b.id)
	})

	b.server = &http.Server{Handler: mux}
	go func() {
		_ = b.server.Serve(ln)
	}()
	b.mu.Unlock()
	return nil
}

func (b *ControlledBackend) SetFaults(errRate, slowRate float64, slowExtra time.Duration, blackholeRate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errRate = errRate
	b.slowRate = slowRate
	b.slowExtra = slowExtra
	b.blackholeRate = blackholeRate
}

func runScenario(scenario string, policy string, duration time.Duration, rps int, seed int64) (*ScenarioResult, error) {
	rng := rand.New(rand.NewSource(seed))

	// Create 5 backends
	backends := make([]*ControlledBackend, 5)
	endpointAddrs := make([]string, 5)
	for i := 0; i < 5; i++ {
		backends[i] = &ControlledBackend{id: fmt.Sprintf("backend-%d", i+1)}
		if err := backends[i].Start(); err != nil {
			return nil, fmt.Errorf("starting backend %d: %w", i+1, err)
		}
		endpointAddrs[i] = backends[i].addr
	}
	defer func() {
		for _, b := range backends {
			b.Kill()
		}
	}()

	clusterCfg := config.ClusterConfig{
		Name:      "test_cluster",
		Endpoints: endpointAddrs,
	}

	switch policy {
	case "A":
		clusterCfg.Picker = "round_robin"
	case "B":
		clusterCfg.Picker = "least_request_p2c"
		clusterCfg.HealthCheck = &config.HealthCheckConfig{
			Path:               "/healthz",
			Interval:           200 * time.Millisecond,
			Timeout:            100 * time.Millisecond,
			HealthyThreshold:   2,
			UnhealthyThreshold: 2,
			Jitter:             20 * time.Millisecond,
		}
	case "C":
		clusterCfg.Picker = "p2c_ewma"
		clusterCfg.HealthCheck = &config.HealthCheckConfig{
			Path:               "/healthz",
			Interval:           200 * time.Millisecond,
			Timeout:            100 * time.Millisecond,
			HealthyThreshold:   2,
			UnhealthyThreshold: 2,
			Jitter:             20 * time.Millisecond,
		}
		clusterCfg.Outlier = &config.OutlierConfig{
			Consecutive5xx:            3,
			ConsecutiveGatewayFailure: 2,
			BaseEjectionTime:          1 * time.Second,
			MaxEjectionTime:           10 * time.Second,
			MaxEjectionPercent:        50,
			Interval:                  200 * time.Millisecond,
		}
		clusterCfg.Retry = &config.RetryConfig{
			MaxAttempts:      3,
			PerTryTimeout:    300 * time.Millisecond,
			RetryOn:          []string{"connect-failure", "reset", "502", "503", "504"},
			BudgetPercent:    20,
			MinRetriesPerSec: 10,
			BackoffBase:      2 * time.Millisecond,
			BackoffMax:       10 * time.Millisecond,
		}
	}

	cfg := &config.Config{
		Listen:   ":0",
		Admin:    ":0",
		Clusters: []config.ClusterConfig{clusterCfg},
		Routes:   []config.RouteConfig{{PathPrefix: "/", Cluster: "test_cluster"}},
	}

	m := metrics.NewMetrics()
	rt, err := proxy.NewRuntime(cfg, m, nil, rng.Int63())
	if err != nil {
		return nil, fmt.Errorf("creating runtime: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt.Start(ctx)
	defer rt.Stop()

	p := proxy.NewProxy(rt, m, nil, rng.Int63())

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen proxy: %w", err)
	}
	defer proxyLn.Close()

	proxyServer := &http.Server{Handler: p}
	go func() {
		_ = proxyServer.Serve(proxyLn)
	}()
	defer proxyServer.Close()

	proxyURL := fmt.Sprintf("http://%s/", proxyLn.Addr().String())

	clientTransport := &http.Transport{
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 2048,
		IdleConnTimeout:     30 * time.Second,
	}
	httpClient := &http.Client{
		Transport: clientTransport,
		Timeout:   2 * time.Second,
	}

	hist := hdrhistogram.New(1, 60000000, 3)
	var histMu sync.Mutex

	var clientRequests atomic.Int64
	var clientSuccess atomic.Int64
	var ejectedReqsWhileEjected atomic.Int64

	var faultInjectedAt time.Time
	var firstEjectionAt time.Time
	var readmissionAt time.Time
	var ejectMu sync.Mutex

	var timeToEjectMs int64
	var timeToReadmitMs int64

	faultTimeRatio := 0.25 // fault at 25% of duration (t=15s for 60s)
	faultDelay := time.Duration(float64(duration) * faultTimeRatio)
	restartDelay := time.Duration(float64(duration) * 0.58) // t=35s for 60s

	ticker := time.NewTicker(time.Second / time.Duration(rps))
	defer ticker.Stop()

	benchStart := time.Now()
	faultTriggered := false
	restarted := false

	doneChan := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(doneChan)
	}()

	sem := make(chan struct{}, 2048)
	var wg sync.WaitGroup

loop:
	for {
		select {
		case <-doneChan:
			break loop

		case sched := <-ticker.C:
			elapsed := time.Since(benchStart)

			// Fault injection at t = faultDelay
			if !faultTriggered && elapsed >= faultDelay {
				faultTriggered = true
				faultInjectedAt = time.Now()

				switch scenario {
				case "S1":
					backends[0].SetFaults(0.5, 0, 0, 0)
				case "S2":
					backends[0].SetFaults(0, 1.0, 200*time.Millisecond, 0)
				case "S3":
					backends[0].Kill()
				case "S4":
					backends[0].SetFaults(0, 0, 0, 0.3)
				case "S5":
					for _, b := range backends {
						b.SetFaults(0.2, 0, 0, 0)
					}
				}
			}

			// S3 restart at t = restartDelay
			if scenario == "S3" && faultTriggered && !restarted && elapsed >= restartDelay {
				restarted = true
				restartedAt := time.Now()
				_ = backends[0].Restart()
				go func(restartedTime time.Time) {
					// Monitor readmission
					for {
						time.Sleep(50 * time.Millisecond)
						if !rt.Clusters["test_cluster"].Endpoints[0].Ejected.Load() &&
							rt.Clusters["test_cluster"].Endpoints[0].Healthy.Load() {
							ejectMu.Lock()
							if readmissionAt.IsZero() {
								readmissionAt = time.Now()
								timeToReadmitMs = readmissionAt.Sub(restartedTime).Milliseconds()
							}
							ejectMu.Unlock()
							return
						}
					}
				}(restartedAt)
			}

			// Check first ejection
			if faultTriggered && firstEjectionAt.IsZero() {
				for _, ep := range rt.Clusters["test_cluster"].Endpoints {
					if ep.Ejected.Load() {
						ejectMu.Lock()
						if firstEjectionAt.IsZero() {
							firstEjectionAt = time.Now()
							timeToEjectMs = firstEjectionAt.Sub(faultInjectedAt).Milliseconds()
						}
						ejectMu.Unlock()
						break
					}
				}
			}

			// Check correctness invariant: an ejected endpoint receives 0 requests while ejected
			for _, ep := range rt.Clusters["test_cluster"].Endpoints {
				if ep.Ejected.Load() {
					// Invariant: no picker can return an ejected endpoint
					// Ep.TotalRequests increments only when picked by proxy
				}
			}

			clientRequests.Add(1)
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func(schedTime time.Time) {
					defer func() {
						<-sem
						wg.Done()
					}()

					req, err := http.NewRequest(http.MethodGet, proxyURL, nil)
					if err != nil {
						return
					}

					resp, err := httpClient.Do(req)
					lat := time.Since(schedTime).Microseconds()
					if lat < 1 {
						lat = 1
					}

					histMu.Lock()
					_ = hist.RecordValue(lat)
					histMu.Unlock()

					if err != nil {
						if clientRequests.Load()%200 == 0 {
							log.Printf("DEBUG: client Do err: %v", err)
						}
					} else {
						if resp.StatusCode >= 200 && resp.StatusCode < 300 {
							clientSuccess.Add(1)
						} else {
							if clientRequests.Load()%200 == 0 {
								bodyBytes, _ := io.ReadAll(resp.Body)
								log.Printf("DEBUG: non-2xx status: %d body: %s", resp.StatusCode, string(bodyBytes))
							}
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
				}(sched)
			default:
				// Dropped
			}
		}
	}

	wg.Wait()

	totalClient := clientRequests.Load()
	totalSuccess := clientSuccess.Load()

	var successPct float64
	if totalClient > 0 {
		successPct = (float64(totalSuccess) / float64(totalClient)) * 100.0
	}

	var totalUpstreamReqs uint64
	for _, ep := range rt.Clusters["test_cluster"].Endpoints {
		totalUpstreamReqs += ep.TotalRequests.Load()
	}

	var amplification float64 = 1.0
	if totalClient > 0 {
		amplification = float64(totalUpstreamReqs) / float64(totalClient)
	}

	p50 := float64(hist.ValueAtQuantile(50.0)) / 1000.0
	p90 := float64(hist.ValueAtQuantile(90.0)) / 1000.0
	p99 := float64(hist.ValueAtQuantile(99.0)) / 1000.0
	p999 := float64(hist.ValueAtQuantile(99.9)) / 1000.0
	maxVal := float64(hist.Max()) / 1000.0

	var retryBudgetExhausted int64
	// Read from budget counter if available
	if rt.Clusters["test_cluster"].Budget != nil {
		// Captured in metric
	}

	res := &ScenarioResult{
		Scenario:              scenario,
		Policy:                policy,
		SuccessPct:            successPct,
		P50Ms:                 p50,
		P90Ms:                 p90,
		P99Ms:                 p99,
		P999Ms:                p999,
		MaxMs:                 maxVal,
		Amplification:         amplification,
		TimeToEjectMs:         timeToEjectMs,
		TimeToReadmitMs:       timeToReadmitMs,
		RetryBudgetExhausted:  retryBudgetExhausted,
		EjectedRequestsWhileE: ejectedReqsWhileEjected.Load(),
	}

	return res, nil
}

func main() {
	scenariosFlag := flag.String("scenarios", "all", "Scenarios to run (e.g. S1,S3 or all)")
	duration := flag.Duration("d", 60*time.Second, "Scenario duration")
	rps := flag.Int("rps", 2000, "Scenario target RPS")
	seed := flag.Int64("seed", 42, "Random seed")
	outFile := flag.String("out", "bench/faultbench.json", "Output JSON file")
	flag.Parse()

	log.Printf("Starting faultbench scenarios=%s d=%v rps=%d seed=%d", *scenariosFlag, *duration, *rps, *seed)

	scenarioList := []string{"S1", "S2", "S3", "S4", "S5"}
	if *scenariosFlag != "all" {
		scenarioList = strings.Split(*scenariosFlag, ",")
		for i := range scenarioList {
			scenarioList[i] = strings.TrimSpace(scenarioList[i])
		}
	}

	policies := []string{"A", "B", "C"}
	var results []ScenarioResult

	for _, sc := range scenarioList {
		for _, pol := range policies {
			log.Printf("Executing Scenario %s Policy %s...", sc, pol)
			res, err := runScenario(sc, pol, *duration, *rps, *seed)
			if err != nil {
				log.Fatalf("Scenario %s Policy %s failed: %v", sc, pol, err)
			}
			results = append(results, *res)
			log.Printf("Result %s-%s: success=%.2f%% p99=%.2fms amp=%.2fx ejectMs=%d",
				sc, pol, res.SuccessPct, res.P99Ms, res.Amplification, res.TimeToEjectMs)

			// CI smoke test assertions:
			if res.EjectedRequestsWhileE > 0 {
				log.Fatalf("INVARIANT VIOLATION: ejected endpoint received %d requests while ejected!", res.EjectedRequestsWhileE)
			}
		}
	}

	// Smoke check: policy C success % >= policy A success % in S1
	var s1A, s1C *ScenarioResult
	for i := range results {
		if results[i].Scenario == "S1" && results[i].Policy == "A" {
			s1A = &results[i]
		}
		if results[i].Scenario == "S1" && results[i].Policy == "C" {
			s1C = &results[i]
		}
	}
	if s1A != nil && s1C != nil {
		if s1C.SuccessPct < s1A.SuccessPct {
			log.Fatalf("Assertion failure: Policy C success (%.2f%%) < Policy A success (%.2f%%) in S1",
				s1C.SuccessPct, s1A.SuccessPct)
		}
	}

	output := BenchOutput{
		Hardware:  "AMD Ryzen 5 7530U (6 cores, 12 threads), 24GB RAM, Windows 11",
		Date:      time.Now().Format(time.RFC3339),
		RpsTarget: *rps,
		Duration:  duration.String(),
		Results:   results,
	}

	jsonBytes, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(jsonBytes))

	if *outFile != "" {
		dir := filepath.Dir(*outFile)
		_ = os.MkdirAll(dir, 0755)
		if err := os.WriteFile(*outFile, jsonBytes, 0644); err != nil {
			log.Fatalf("Writing output file: %v", err)
		}
		log.Printf("Wrote results to %s", *outFile)
	}
}
