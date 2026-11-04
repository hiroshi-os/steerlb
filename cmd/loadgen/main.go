package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

type Report struct {
	Target         string           `json:"target"`
	RpsTarget      int              `json:"rps_target"`
	RpsAchieved    float64          `json:"rps_achieved"`
	DurationSec    float64          `json:"duration_sec"`
	RequestsTotal  int64            `json:"requests_total"`
	SuccessTotal   int64            `json:"success_total"`
	SuccessPercent float64          `json:"success_percent"`
	P50Microsec    int64            `json:"p50_us"`
	P90Microsec    int64            `json:"p90_us"`
	P99Microsec    int64            `json:"p99_us"`
	P999Microsec   int64            `json:"p999_us"`
	MaxMicrosec    int64            `json:"max_us"`
	BackendCounts  map[string]int64 `json:"backend_counts"`
}

func main() {
	target := flag.String("target", "http://127.0.0.1:8080/", "Target URL")
	rps := flag.Int("rps", 1000, "Target requests per second")
	duration := flag.Duration("d", 10*time.Second, "Duration of the benchmark")
	concurrency := flag.Int("c", 128, "Max concurrent in-flight requests")
	timeout := flag.Duration("timeout", 5*time.Second, "Per-request client timeout")
	seed := flag.Int64("seed", 1, "Random seed (for repeatability)")
	outFile := flag.String("out", "", "Output JSON file (optional)")
	flag.Parse()

	_ = seed

	if *rps <= 0 {
		fmt.Fprintf(os.Stderr, "rps must be > 0\n")
		os.Exit(1)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   *timeout,
	}

	// 1 microsecond to 60 seconds range, 3 significant figures
	hist := hdrhistogram.New(1, 60000000, 3)
	var histMu sync.Mutex

	var totalRequests atomic.Int64
	var successRequests atomic.Int64
	var backendMu sync.Mutex
	backendCounts := make(map[string]int64)

	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	interval := time.Second / time.Duration(*rps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	benchStart := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case schedTime := <-ticker.C:
			totalRequests.Add(1)

			// Non-blocking concurrency acquisition to maintain open loop
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go func(sched time.Time) {
					defer func() {
						<-sem
						wg.Done()
					}()

					req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, *target, nil)
					if err != nil {
						return
					}

					resp, err := client.Do(req)
					elapsed := time.Since(sched)

					us := elapsed.Microseconds()
					if us < 1 {
						us = 1
					} else if us > 60000000 {
						us = 60000000
					}

					histMu.Lock()
					_ = hist.RecordValue(us)
					histMu.Unlock()

					if err == nil {
						if resp.StatusCode >= 200 && resp.StatusCode < 300 {
							successRequests.Add(1)
						}
						bID := resp.Header.Get("X-Backend-Id")
						if bID != "" {
							backendMu.Lock()
							backendCounts[bID]++
							backendMu.Unlock()
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
				}(schedTime)

			default:
				// Concurrency limit reached: count as dropped / overloaded
				histMu.Lock()
				_ = hist.RecordValue(timeout.Microseconds())
				histMu.Unlock()
			}
		}
	}

	wg.Wait()
	actualDuration := time.Since(benchStart).Seconds()

	total := totalRequests.Load()
	success := successRequests.Load()
	var successPct float64
	if total > 0 {
		successPct = (float64(success) / float64(total)) * 100.0
	}
	rpsAchieved := float64(total) / actualDuration

	p50 := hist.ValueAtQuantile(50.0)
	p90 := hist.ValueAtQuantile(90.0)
	p99 := hist.ValueAtQuantile(99.0)
	p999 := hist.ValueAtQuantile(99.9)
	maxVal := hist.Max()

	report := Report{
		Target:         *target,
		RpsTarget:      *rps,
		RpsAchieved:    rpsAchieved,
		DurationSec:    actualDuration,
		RequestsTotal:  total,
		SuccessTotal:   success,
		SuccessPercent: successPct,
		P50Microsec:    p50,
		P90Microsec:    p90,
		P99Microsec:    p99,
		P999Microsec:   p999,
		MaxMicrosec:    maxVal,
		BackendCounts:  backendCounts,
	}

	jsonBytes, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(jsonBytes))

	// Markdown table row
	fmt.Printf("\n| Target | Target RPS | Achieved RPS | Success %% | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|---|\n")
	fmt.Printf("| %s | %d | %.1f | %.2f%% | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
		*target, *rps, rpsAchieved, successPct,
		float64(p50)/1000.0, float64(p90)/1000.0, float64(p99)/1000.0, float64(p999)/1000.0, float64(maxVal)/1000.0)

	if *outFile != "" {
		_ = os.WriteFile(*outFile, jsonBytes, 0644)
	}
}
